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

package v1alpha

import (
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// ClusterJobSpec defines the desired state of ClusterJob
type ClusterJobSpec struct {
	// Strategy defines how the job should be executed
	Strategy *ClusterJobStrategy `json:"strategy"`

	// FailureStrategy defines how to handle failures (keepgoing or exit)
	// +kubebuilder:validation:Enum=keepgoing;exit
	// +kubebuilder:default:=keepgoing
	FailureStrategy string `json:"failureStrategy,omitempty"`

	// +required
	JobTemplate *batchv1.JobTemplateSpec `json:"jobTemplate"`
}

// ClusterJobStrategy holds job execution strategy details
type ClusterJobStrategy struct {
	// Type specifies which strategy is used: batch, perNodeGroup or all
	// +kubebuilder:validation:Enum=batch;perNodeGroup;all
	Type string `json:"type"`

	// Batch execution strategy (valid only when Type=batch)
	// +optional
	Batch *BatchStrategy `json:"batch,omitempty"`

	// PerNodeGroup execution strategy (valid only when Type=perNodeGroup)
	// +optional
	PerNodeGroup *PerNodeGroupStrategy `json:"perNodeGroup,omitempty"`
}

// BatchStrategy defines fields for parallel execution
type BatchStrategy struct {
	// Order defines grouping order (random or alphabetical)
	// +kubebuilder:validation:Enum=random;alphabetical
	// +kubebuilder:default:=random
	Order string `json:"order,omitempty"`

	// Size defines group size for parallel execution
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default:=1
	Size int32 `json:"size,omitempty"`
}

// PerNodeGroupStrategy defines fields for per-node-group execution
type PerNodeGroupStrategy struct {
	// GroupLabel is the node label key to use for grouping
	// +required
	GroupLabel string `json:"groupLabel"`

	// IgnoreNull specifies whether to ignore nodes without the groupLabel (true),
	// or treat them as a separate group (false)
	IgnoreNull bool `json:"ignoreNull,omitempty"`
}

// ClusterJobStatus defines the observed state of ClusterJob.
type ClusterJobStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the ClusterJob resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// NodeGroups stores execution unit groups and their member nodes
	NodeGroups []NodeGroupStatus `json:"nodeGroups,omitempty"`

	// This status is phase of ClustertJob, phase is one of prepering, running, completed, or failed.
	// +kubebuilder:validation:Enum=Prepering;Running;Completed;Failed
	Phase           string `json:"phase,omitempty"`
	CurrentGroup    string `json:"currentGroup,omitempty"`
	CurrentIndex    int    `json:"currentIndex"`
	CompletedGroups int    `json:"completedGroups"`
	FailedGroups    int    `json:"failedGroups"`
	WaitGroups      int    `json:"waitGroups"`
}

// NodeGroupStatus represents a single execution unit group in status
type NodeGroupStatus struct {
	Name  string   `json:"name"`  // Nodegroup name
	Nodes []string `json:"nodes"` // Nodes belonging to that group
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// +kubebuilder:printcolumn:name="Phase",type="string",JSONPath=".status.phase",description="Phase of ClsterJob"
// +kubebuilder:printcolumn:name="CurrentGroup",type="string",JSONPath=".status.currentGroup",description="The group in which the ClusterJob is currently running"
// ClusterJob is the Schema for the clusterjobs API
type ClusterJob struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty,omitzero"`

	// spec defines the desired state of ClusterJob
	// +required
	Spec ClusterJobSpec `json:"spec"`

	// status defines the observed state of ClusterJob
	// +optional
	Status ClusterJobStatus `json:"status,omitempty,omitzero"`
}

// +kubebuilder:object:root=true

// ClusterJobList contains a list of ClusterJob
type ClusterJobList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ClusterJob `json:"items"`
}

func init() {
	SchemeBuilder.Register(&ClusterJob{}, &ClusterJobList{})
}

func (cj *ClusterJob) UpdatePreperingStatus(nodeGroups []NodeGroupStatus) {
	cj.Status.Phase = "Prepering"
	cj.Status.CurrentIndex = 0
	cj.Status.CurrentGroup = nodeGroups[0].Name
	cj.Status.WaitGroups = len(nodeGroups)
	cj.Status.NodeGroups = nodeGroups
}

func (cj *ClusterJob) PrepareNextGroup(succeeded bool) {
	cj.Status.Phase = "Prepering"
	cj.Status.CurrentIndex = cj.Status.CurrentIndex + 1
	cj.Status.CurrentGroup = cj.Status.NodeGroups[cj.Status.CurrentIndex].Name

	if succeeded {
		cj.Status.CompletedGroups += 1
	} else {
		cj.Status.FailedGroups += 1
	}
}

func (cj *ClusterJob) UpdateRunningStatus() {
	cj.Status.Phase = "Running"
	cj.Status.WaitGroups = len(cj.Status.NodeGroups) - cj.Status.CurrentIndex - 1
}

func (cj *ClusterJob) UpdateCompletedStatus() {
	if cj.Status.FailedGroups > 0 {
		cj.UpdateFailedStatus()
	} else {
		cj.Status.Phase = "Completed"
	}
}

func (cj *ClusterJob) UpdateFailedStatus() {
	cj.Status.Phase = "Failed"
}

func (cj *ClusterJob) CreateNodeGroup(groupSuffix string, nodes []string) NodeGroupStatus {
	return NodeGroupStatus{
		Name:  fmt.Sprintf("%s-%s", cj.Name, groupSuffix),
		Nodes: nodes,
	}
}
