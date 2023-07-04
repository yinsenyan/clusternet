/*
Copyright 2022 The Clusternet Authors.

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

package defaultassigner

import (
	"context"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/klog/v2"
	"k8s.io/utils/pointer"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/names"
	"github.com/clusternet/clusternet/pkg/utils"
)

const (
	// NameDynamicAssigner is the name of the plugin used in the plugin registry and configurations.
	NameDynamicAssigner = names.DynamicAssigner

	// preAssignStateKey is the key in CycleState to DynamicAssigner pre-computed data.
	// Using the name of the plugin will likely help us avoid collisions with other plugins.
	preAssignStateKey = "PreAssign" + names.DynamicAssigner
)

// DynamicAssigner assigns replicas to clusters.
type DynamicAssigner struct {
	handle framework.Handle
}

var _ framework.AssignPlugin = &DynamicAssigner{}
var _ framework.PreAssignPlugin = &DynamicAssigner{}

// preAssignState computed at PreAssign and used at Assign.
type PreAssignState struct {
	Deviations []appsapi.FeedOrder
}

// Clone the prefilter state.
func (s *PreAssignState) Clone() framework.StateData {
	return s
}

func getPreAssignState(cycleState *framework.CycleState) (*PreAssignState, error) {
	c, err := cycleState.Read(preAssignStateKey)
	if err != nil {
		// preAssignState doesn't exist, likely PreAssign wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preAssignStateKey, err)
	}

	s, ok := c.(*PreAssignState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to DynamicAssigner.preAssignState error", c)
	}
	return s, nil
}

// NewDynamicAssigner creates a DefaultAssigner.
func NewDynamicAssigner(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &DynamicAssigner{handle: handle}, nil
}

// Name returns the name of the plugin.
func (pl *DynamicAssigner) Name() string {
	return NameDynamicAssigner
}

func (pl *DynamicAssigner) PreAssign(ctx context.Context, state *framework.CycleState, sub *appsapi.Subscription, finv *appsapi.FeedInventory, availableReplicas framework.TargetClusters) *framework.Status {
	if sub.Spec.DividingScheduling == nil || sub.Spec.DividingScheduling.Type != appsapi.DynamicReplicaDividingType {
		return nil
	}
	if sub.Spec.DividingScheduling.DynamicDividing == nil {
		return framework.AsStatus(fmt.Errorf("must specify field DynamicDividing when dividing type is dynamic"))
	}

	// Scheduler only cares about the deviation replicas in case rescheduling running replicas.
	// When scaling down, the deviation replicas would be negative.
	s := new(PreAssignState)
	if metav1.HasAnnotation(sub.ObjectMeta, known.AnnoEnableTopology) && !metav1.HasAnnotation(sub.ObjectMeta, known.AnnoTopologyReplicas) {
		s.Deviations = finv.Spec.Feeds
		state.Write(preAssignStateKey, s)
		return nil
	}

	deviations := make([]appsapi.FeedOrder, len(finv.Spec.Feeds))
	for i := range finv.Spec.Feeds {
		deviations[i] = *finv.Spec.Feeds[i].DeepCopy()
		if deviations[i].DesiredReplicas == nil {
			continue
		}
		currentReplicas := utils.SumArrayInt32(sub.Status.Replicas[utils.GetFeedKey(deviations[i].Feed)])
		deviations[i].DesiredReplicas = pointer.Int32(*deviations[i].DesiredReplicas - currentReplicas)
	}
	s.Deviations = deviations

	state.Write(preAssignStateKey, s)
	return nil
}

// Assign assigns subscriptions to clusters using the clusternet client.
func (pl *DynamicAssigner) Assign(ctx context.Context, state *framework.CycleState, sub *appsapi.Subscription, finv *appsapi.FeedInventory, availableReplicas framework.TargetClusters) (framework.TargetClusters, *framework.Status) {
	klog.V(5).InfoS("Attempting to assign replicas to clusters",
		"subscription", klog.KObj(sub), "clusters", availableReplicas.BindingClusters)
	if sub.Spec.DividingScheduling == nil || sub.Spec.DividingScheduling.Type != appsapi.DynamicReplicaDividingType {
		klog.V(5).Infof("sub %s/%s will skip assigner %s because of scheduling not match ", sub.Namespace, sub.Name, pl.Name())
		return framework.TargetClusters{}, framework.NewStatus(framework.Skip, "")
	}
	if metav1.HasAnnotation(sub.ObjectMeta, known.AnnoEnableTopology) {
		klog.V(5).Infof("sub %s/%s will skip assigner %s because enable topology scheduling", sub.Namespace, sub.Name, pl.Name())
		return framework.TargetClusters{}, framework.NewStatus(framework.Skip, "")
	}
	s, err := getPreAssignState(state)
	if err != nil {
		return framework.TargetClusters{}, framework.AsStatus(err)
	}

	result, err := DynamicDivideReplicas(sub, s.Deviations, availableReplicas)
	if err != nil {
		return framework.TargetClusters{}, framework.AsStatus(err)
	}

	return result, nil
}
