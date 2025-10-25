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
	ClusterJobNodeLabel     = "clusterjob/targer-node"
	ClusterJobNullGroupName = "null-group"
)

// ClusterJobReconciler reconciles a ClusterJob object
type ClusterJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=cluster-batch.beer1.com,resources=clusterjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=cluster-batch.beer1.com,resources=clusterjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=cluster-batch.beer1.com,resources=clusterjobs/finalizers,verbs=update

// +kubebuilder:rbac:groups="",resources=nodes,verbs=get;list;watch
// +kubebuilder:rbac:groups="batch",resources=jobs,verbs=get;list;watch;create

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

	var clusterJob clusterbatchv1alpha.ClusterJob

	// 1. ClusterJob 조회
	if err := r.Get(ctx, req.NamespacedName, &clusterJob); err != nil {
		// ClusterJob이 이미 삭제된 경우 핸들링
		logger.Info("ClusterJob is deleted", "namespace", clusterJob.Namespace, "name", clusterJob.Name)
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	switch clusterJob.Status.Phase {
	case "":
		// Group 스케줄링 되어있지 않으면 스케줄링 진행
		logger.Info("ClusterJob is not grouped. Grouping started.. ", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "strategyType", clusterJob.Spec.Strategy.Type)
		if err := r.makeNodeGroups(ctx, &clusterJob); err != nil {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	case "Prepering":
		logger.Info("ClusterJob is already grouped.", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "phase", clusterJob.Status.Phase)

		if err := r.scheduleCurrentNodeGroup(ctx, &clusterJob); err != nil {
			return ctrl.Result{RequeueAfter: 10 * time.Second}, err
		}
	case "Running":
		// TODO: Handle Running phase
	case "Completed":
		// TODO: Handle Completed phase
	case "Failed":
		// TODO: Handle Running phase
	default:
		// TODO: Handle Exception
	}

	// ClusterJob의 변경된 Status 저장
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
	case "all":
		nodeGroups, err = createAllNodeGroups(ctx, clusterJob, nodeList)
	case "batch":
		nodeGroups, err = createBatchNodeGroups(ctx, clusterJob, nodeList)
	case "perNodeGroup":
		nodeGroups, err = createPerNodeGroupGroups(ctx, clusterJob, nodeList)
	default:
		return fmt.Errorf("unsupported strategy type: %s", clusterJob.Spec.Strategy.Type)
	}

	if err != nil {
		return fmt.Errorf("failed to create node groups: %w", err)
	}

	// 노드그룹을 ClusterJob status에 저장 및 Prepering 상태로 변경
	clusterJob.UpdatePreperingStatus(nodeGroups)

	logger.Info("Node groups created successfully",
		"namespace", clusterJob.Namespace,
		"name", clusterJob.Name,
		"strategy", clusterJob.Spec.Strategy.Type,
		"totalGroups", len(nodeGroups))

	return nil
}

// getNodeList는 클러스터의 모든 노드 목록을 조회한다.
func (r *ClusterJobReconciler) getNodeList(ctx context.Context) (*corev1.NodeList, error) {
	var nodeList corev1.NodeList
	if err := r.Client.List(ctx, &nodeList); err != nil {
		return nil, err
	}
	return &nodeList, nil
}

// createAllNodeGroups는 All 전략에 따라 노드 그룹을 생성한다.
func createAllNodeGroups(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob, nodeList *corev1.NodeList) ([]clusterbatchv1alpha.NodeGroupStatus, error) {
	var logger = logf.FromContext(ctx)
	logger.Info("Creating Batch node groups", "namespace", clusterJob.Namespace, "name", clusterJob.Name)
	// 노드 이름 추출
	nodeNames := extractNodeNames(nodeList)
	// 그룹 생성
	var nodeGroups []clusterbatchv1alpha.NodeGroupStatus

	group := clusterJob.CreateNodeGroup("all", nodeNames)
	nodeGroups = append(nodeGroups, group)

	return nodeGroups, nil
}

// createBatchNodeGroups는 Batch 전략에 따라 노드 그룹을 생성한다.
// Batch 전략은 정해진 크기(Batch.Size)를 기준으로 노드그룹을 묶어서 생성한다.
// order가 random인 경우 노드 이름을 무작위로 섞고, alphabetical인 경우 노드그룹을 알파벳 순으로 묶어서 생성한다.
func createBatchNodeGroups(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob, nodeList *corev1.NodeList) ([]clusterbatchv1alpha.NodeGroupStatus, error) {
	var logger = logf.FromContext(ctx)
	logger.Info("Creating Batch node groups", "namespace", clusterJob.Namespace, "name", clusterJob.Name)

	size := clusterJob.Spec.Strategy.Batch.Size
	order := clusterJob.Spec.Strategy.Batch.Order

	// 노드 이름 추출
	nodeNames := extractNodeNames(nodeList)

	// order가 random인 경우 노드 이름을 무작위로 섞는다.
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

// createPerNodeGroupGroups는 perNodeGroup 전략에 따라 노드 그룹을 생성한다.
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

// extractNodeNames는 노드 목록에서 노드 이름들을 추출한다.
func extractNodeNames(nodeList *corev1.NodeList) []string {
	nodeNames := make([]string, len(nodeList.Items))
	for i, node := range nodeList.Items {
		nodeNames[i] = node.Name
	}
	return nodeNames
}

// shuffleNodeNames는 노드 이름 목록을 무작위로 섞는다.
func shuffleNodeNames(nodeNames []string) {
	randSource := rand.New(rand.NewSource(time.Now().UnixNano()))
	randSource.Shuffle(len(nodeNames), func(i, j int) {
		nodeNames[i], nodeNames[j] = nodeNames[j], nodeNames[i]
	})
}

// groupNodesByLabel은 노드를 레이블 값에 따라 그룹화한다.
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

// scheduleNextNodeGroup는 ClusterJob의 현재 실행 노드그룹에 잡을 실행시킨다.
func (r *ClusterJobReconciler) scheduleCurrentNodeGroup(ctx context.Context, clusterJob *clusterbatchv1alpha.ClusterJob) error {
	var logger = logf.FromContext(ctx)

	var groups = clusterJob.Status.NodeGroups

	// 실행 노드그룹의 인덱스를 넘어간 경우는 더 이상 실행할 노드그룹이 없다는 뜻이므로 완료 처리
	if clusterJob.Status.CurrentIndex >= len(groups) {
		clusterJob.UpdateCompletedStatus()
		return nil
	}

	var nextGroup = groups[clusterJob.Status.CurrentIndex]

	// 이미 해당 그룹의 Job들이 생성되어 있는지 확인
	var existingJobList batchv1.JobList

	labelSelector := client.MatchingLabels{
		ClusterJobNameLabel:  clusterJob.Name,
		ClusterJobGroupLabel: nextGroup.Name,
	}

	if err := r.List(ctx, &existingJobList, client.InNamespace(clusterJob.Namespace), labelSelector); err != nil {
		return err
	}

	// 이미 잡이 생성된 노드를 판별하는 맵
	jobCreatedNodes := map[string]bool{}

	for _, job := range existingJobList.Items {
		jobCreatedNodes[job.ObjectMeta.Labels[ClusterJobNodeLabel]] = true
	}

	errors := 0 

	for index, node := range nextGroup.Nodes {

		// 특정 노드에 잡이 이미 생성되어있다면 스킵
		if jobCreatedNodes[node] {
			logger.Info("Job is already created.", "node", node)
			continue
		}

		var jobSpec = clusterJob.Spec.JobTemplate.Spec

		jobSpec.Template.Spec.NodeName = node

		// 특정 워커노드로 배포되는 잡에 레이블 부여하기 위해 key값 추가
		labelSelector[ClusterJobNodeLabel] = node

		job := &batchv1.Job{
			ObjectMeta: metav1.ObjectMeta{
				GenerateName: fmt.Sprintf("%s-%s-%d-", clusterJob.Name, nextGroup.Name, index),
				Namespace:    clusterJob.Namespace,
				Labels:       labelSelector,
				OwnerReferences: []metav1.OwnerReference{
					{
						APIVersion:         clusterJob.APIVersion,
						Kind:               clusterJob.Kind,
						Name:               clusterJob.Name,
						UID:                clusterJob.ObjectMeta.UID,
						Controller:         &[]bool{true}[0],
						BlockOwnerDeletion: &[]bool{true}[0],
					},
				},
			},
			Spec: jobSpec,
		}

		// Job 생성에 실패하면 다음 Reconcile 때 처리
		if err := r.Create(ctx, job); err != nil {
			errors += 1
			logger.Error(err, "Job creation failed.", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "jobName", job.ObjectMeta.GenerateName, "node", node)
		} else {
			logger.Info("Job is created.", "created", "namespace", clusterJob.Namespace, "name", clusterJob.Name, "jobName", job.ObjectMeta.GenerateName, "node", node)
		}
	}
	
	// Job 생성 에러가 없는 경우에만 Running Status로 변경
	if errors == 0 {
		clusterJob.UpdateRunningStatus()
	}

	return nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ClusterJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&clusterbatchv1alpha.ClusterJob{}).
		Named("clusterjob").
		Complete(r)
}
