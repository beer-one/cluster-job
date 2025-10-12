/*
Copyright 2025.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	clusterbatchv1alpha "github.com/beer-one/cluster-job/api/v1alpha"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

const (
	ClusterJobNameLabel     = "clusterjob/name"
	ClusterJobGroupLabel    = "clusterjob/group-name"
	ClusterJobNullGroupName = "null-group"
)

// ClusterJobReconciler reconciles a ClusterJob object
type ClusterJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

type GroupJobStatus struct {
	TotalCount     int
	FailedCount    int
	CompletedCount int
}

// +kubebuilder:rbac:groups=cluster-batch.beer1.com,resources=clusterjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster-batch.beer1.com,resources=clusterjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster-batch.beer1.com,resources=clusterjobs/finalizers,verbs=update

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="batch",resources=jobs,verbs=get;list;watch;create;update;delete

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ClusterJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.22.1/pkg/reconcile
func (r *ClusterJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	var logger = logf.FromContext(ctx)

	// 1. get CR
	var clusterJob clusterbatchv1alpha.ClusterJob

	// CR이 삭제된 경우 핸들링
	if err := r.Get(ctx, req.NamespacedName, &clusterJob); err != nil {
		logger.Info("ClusterJob is deleted", "namespace", clusterJob.Namespace, "name", clusterJob.Name)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// Group 스케줄링 되어있지 않으면 스케줄링 진행
	switch clusterJob.Status.Phase {
	case "":
		logger.Info("ClusterJob is not grouped. Grouping started.. ", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "strategyType", clusterJob.Spec.Strategy.Type)
		if err := r.makeNodeGroups(ctx, &clusterJob); err != nil {
			return ctrl.Result{}, err
		}
	case "Pending":
		logger.Info("ClusterJob is already grouped.", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "phase", clusterJob.Status.Phase)

		if err := r.scheduleNextNodeGroup(ctx, &clusterJob); err != nil {
			return ctrl.Result{}, err
		}
	case "Running":

		status, err := r.checkClusterJobGroupStatus(ctx, &clusterJob)

		if err != nil {
			return ctrl.Result{}, err
		}

		switch status.TotalCount {
		case status.CompletedCount: // 현재 그룹잡 모두 성공적으로 완료
			if err := r.scheduleNextNodeGroup(ctx, &clusterJob); err != nil {
				return ctrl.Result{}, err
			}
		case status.CompletedCount + status.FailedCount: // 현재 그룹잡 모두 실행은 완료

			if clusterJob.Spec.FailureStrategy == "keepgoing" {
				if err := r.scheduleNextNodeGroup(ctx, &clusterJob); err != nil {
					return ctrl.Result{}, err
				}
			} else {
				clusterJob.SaveFailedStatus()
				logger.Info("ClusterJob failed", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "group", clusterJob.Status.CurrentGroup, "failureStrategy", clusterJob.Spec.FailureStrategy)
			}
		default: // 그룹이 생성한 잡 중 실행완료되지 않은 잡이 있는 경우
			return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
		}

	case "Completed":
		logger.Info("ClusterJob completed", "namespace", clusterJob.Namespace, "name", clusterJob.Name)
		return ctrl.Result{}, nil
	case "Failed":
		logger.Info("ClusterJob failed", "namespace", clusterJob.Namespace, "name", clusterJob.Name)
		return ctrl.Result{}, nil
	default:
		err := errors.New("invalid status")
		logger.Error(err, "invalid status")
		return ctrl.Result{}, err
	}

	if err := r.Status().Update(ctx, &clusterJob); err != nil {
		logger.Error(err, "unable to update ClusterJob status")
		// Status 업데이트 실패 시 재시도하되, 너무 빈번하지 않도록 지연 시간 설정
		return ctrl.Result{RequeueAfter: 1 * time.Second}, err
	}
	logger.Info("ClusterJob status update completed")

	// Completed나 Failed 상태가 아닌 경우에만 requeue
	if clusterJob.Status.Phase != "Completed" && clusterJob.Status.Phase != "Failed" {
		return ctrl.Result{RequeueAfter: 100 * time.Millisecond}, nil
	}

	return ctrl.Result{}, nil
}

func (r *ClusterJobReconciler) makeNodeGroups(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob) error {
	var logger = logf.FromContext(ctx)

	// 노드 목록 조회
	nodeList, err := r.getNodeList(ctx)
	if err != nil {
		return fmt.Errorf("failed to get node list: %w", err)
	}

	// 전략에 따른 노드 그룹 생성
	var nodeGroups []clusterbatchv1alpha.NodeGroupStatus
	switch clusterJob.Spec.Strategy.Type {
	case "parallel":
		nodeGroups, err = createParallelNodeGroups(ctx, clusterJob, nodeList)
	case "perNodeGroup":
		nodeGroups, err = createPerNodeGroupGroups(ctx, clusterJob, nodeList)
	default:
		return fmt.Errorf("unsupported strategy type: %s", clusterJob.Spec.Strategy.Type)
	}

	if err != nil {
		return fmt.Errorf("failed to create node groups: %w", err)
	}

	// 노드그룹을 ClusterJob 상태에 저장
	clusterJob.SavePendingStatus(nodeGroups)

	logger.Info("Node groups created successfully",
		"namespace", clusterJob.Namespace,
		"name", clusterJob.Name,
		"strategy", clusterJob.Spec.Strategy.Type,
		"totalGroups", len(nodeGroups))

	return nil
}

// getNodeList는 클러스터의 모든 노드 목록을 조회합니다
func (r *ClusterJobReconciler) getNodeList(ctx context.Context) (*corev1.NodeList, error) {
	var nodeList corev1.NodeList
	if err := r.Client.List(ctx, &nodeList); err != nil {
		return nil, err
	}
	return &nodeList, nil
}

// createParallelNodeGroups는 parallel 전략에 따라 노드 그룹을 생성합니다.
// parallel 전략은 정해진 크기(Parallel.Size)를 기준으로 노드그룹을 묶어서 생성합니다.
// order가 random인 경우 노드 이름을 무작위로 섞고, alphabetical인 경우 노드그룹을 알파벳 순으로 묶어서 생성합니다.
func createParallelNodeGroups(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob, nodeList *corev1.NodeList) ([]clusterbatchv1alpha.NodeGroupStatus, error) {
	var logger = logf.FromContext(ctx)
	logger.Info("Creating parallel node groups", "namespace", clusterJob.Namespace, "name", clusterJob.Name)

	size := clusterJob.Spec.Strategy.Parallel.Size
	order := clusterJob.Spec.Strategy.Parallel.Order

	// 노드 이름 추출
	nodeNames := extractNodeNames(nodeList)

	// order가 random인 경우 노드 이름을 무작위로 섞습니다.
	if order == "random" {
		shuffleNodeNames(nodeNames)
	}

	// 그룹 생성
	var nodeGroups []clusterbatchv1alpha.NodeGroupStatus
	groupIndex := 0

	for start := 0; start < len(nodeNames); start += int(size) {
		end := min(len(nodeNames), start+int(size))
		group := clusterJob.CreateNodeGroup(strconv.Itoa(groupIndex), nodeNames[start:end])
		nodeGroups = append(nodeGroups, group)
		groupIndex += 1
	}

	return nodeGroups, nil
}

// createPerNodeGroupGroups는 perNodeGroup 전략에 따라 노드 그룹을 생성합니다
func createPerNodeGroupGroups(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob, nodeList *corev1.NodeList) ([]clusterbatchv1alpha.NodeGroupStatus, error) {
	var logger = logf.FromContext(ctx)
	logger.Info("Creating per-node-group groups", "namespace", clusterJob.Namespace, "name", clusterJob.Name)

	groupLabel := clusterJob.Spec.Strategy.PerNodeGroup.GroupLabel
	ignoreNull := clusterJob.Spec.Strategy.PerNodeGroup.IgnoreNull

	// 레이블별로 노드 그룹화
	nodesByGroupLabel := groupNodesByLabel(nodeList, groupLabel, ignoreNull)

	// 그룹 생성
	var nodeGroups []clusterbatchv1alpha.NodeGroupStatus
	for groupName, nodes := range nodesByGroupLabel {
		group := clusterJob.CreateNodeGroup(groupName, nodes)
		nodeGroups = append(nodeGroups, group)
	}

	return nodeGroups, nil
}

// extractNodeNames는 노드 목록에서 노드 이름들을 추출합니다
func extractNodeNames(nodeList *corev1.NodeList) []string {
	nodeNames := make([]string, len(nodeList.Items))
	for i, node := range nodeList.Items {
		nodeNames[i] = node.Name
	}
	return nodeNames
}

// shuffleNodeNames는 노드 이름 목록을 무작위로 섞습니다
func shuffleNodeNames(nodeNames []string) {
	randSource := rand.New(rand.NewSource(time.Now().UnixNano()))
	randSource.Shuffle(len(nodeNames), func(i, j int) {
		nodeNames[i], nodeNames[j] = nodeNames[j], nodeNames[i]
	})
}

// groupNodesByLabel은 노드를 레이블 값에 따라 그룹화합니다
func groupNodesByLabel(nodeList *corev1.NodeList, groupLabel string, ignoreNull bool) map[string][]string {
	nodesByGroupLabel := make(map[string][]string)

	for _, node := range nodeList.Items {
		labelValue, ok := node.Labels[groupLabel]
		groupKey := labelValue

		if !ok && !ignoreNull {
			groupKey = ClusterJobNullGroupName
		} else if !ok && ignoreNull {
			continue
		}

		nodesByGroupLabel[groupKey] = append(nodesByGroupLabel[groupKey], node.Name)
	}

	return nodesByGroupLabel
}

// Execute the job on the next node group.
func (r *ClusterJobReconciler) scheduleNextNodeGroup(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob) error {
	var logger = logf.FromContext(ctx)

	var groups = clusterJob.Status.NodeGroups
	var nextIndex = clusterJob.Status.CurrentIndex + 1

	if nextIndex >= len(groups) {
		clusterJob.SaveCompletedStatus()
		return nil
	}

	var nextGroup = groups[nextIndex]

	// 이미 해당 그룹의 Job들이 생성되어 있는지 확인
	var existingJobList batchv1.JobList
	labelSelector := client.MatchingLabels{
		ClusterJobNameLabel:  clusterJob.Name,
		ClusterJobGroupLabel: nextGroup.Name,
	}
	if err := r.List(ctx, &existingJobList, client.InNamespace(clusterJob.Namespace), labelSelector); err != nil {
		return err
	}

	// 이미 Job들이 존재하면 생성하지 않음
	if len(existingJobList.Items) > 0 {
		logger.Info("Jobs already exist for this group, skipping creation", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "group", nextGroup.Name, "existingJobs", len(existingJobList.Items))
	} else {
		for index, node := range nextGroup.Nodes {

			var jobSpec = clusterJob.Spec.JobTemplate.Spec

			jobSpec.Template.Spec.NodeName = node

			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					GenerateName: fmt.Sprintf("%s-%s-%d-", clusterJob.Name, nextGroup.Name, index),
					Namespace:    clusterJob.Namespace,
					Labels:       labelSelector,
				},
				Spec: jobSpec,
			}
			// TODO: Error handling (그룹 중 하나라도 실패하는 경우 reconcile 생각하기)
			if err := r.Create(ctx, job); err != nil {
				return err
			}

			logger.Info("Job is created.", "created", index+1, "total", len(nextGroup.Nodes), "namespace", clusterJob.Namespace, "name", clusterJob.Name, "jobName", job.ObjectMeta.GenerateName)
		}
	}

	clusterJob.SaveRunningStatus(nextIndex, nextGroup.Name, clusterJob.Status.WaitGroups-1)
	return nil
}

func (r *ClusterJobReconciler) cleanup(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob) error {
	var jobList batchv1.JobList

	labelSelector := client.MatchingLabels{
		ClusterJobNameLabel:  clusterJob.Name,
		ClusterJobGroupLabel: clusterJob.Status.CurrentGroup,
	}

	// ClusterJob이 만든 Job 조회
	if err := r.List(ctx, &jobList, client.InNamespace(clusterJob.Namespace), labelSelector); err != nil {
		return err
	}

	for _, job := range jobList.Items {
		if err := r.Delete(ctx, &job, &client.DeleteOptions{GracePeriodSeconds: new(int64)}); err != nil {
			return err
		}
	}

	return nil
}

// ClusterJob의 현재 그룹의 잡 실행 상태를 취합하여 결과를 반환하는 함수
// GroupJobStatus는 그룹 내 잡의 총 개수, 성공 개수, 실패 개수를 포함하는 구조체
func (r *ClusterJobReconciler) checkClusterJobGroupStatus(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob) (GroupJobStatus, error) {
	// var logger = logf.FromContext(ctx)

	var jobList batchv1.JobList

	labelSelector := client.MatchingLabels{
		ClusterJobNameLabel:  clusterJob.Name,
		ClusterJobGroupLabel: clusterJob.Status.CurrentGroup,
	}

	// ClusterJob이 만든 Job 조회
	if err := r.List(ctx, &jobList, client.InNamespace(clusterJob.Namespace), labelSelector); err != nil {
		return GroupJobStatus{}, err
	}

	completedCount := 0
	failedCount := 0

	for _, job := range jobList.Items {

		backoffLimit := job.Spec.BackoffLimit
		succeeded := job.Status.Succeeded
		failed := job.Status.Failed

		if succeeded > 0 {
			completedCount += 1
		}

		if failed == *backoffLimit+1 {
			failedCount += 1
		}
	}

	return GroupJobStatus{TotalCount: len(jobList.Items), CompletedCount: completedCount, FailedCount: failedCount}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterbatchv1alpha.ClusterJob{}).
		Named("clusterjob").
		Complete(r)
}
