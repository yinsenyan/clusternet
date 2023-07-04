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

package topologyassigner

import (
	"context"
	"encoding/json"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	klog "k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/names"
	"github.com/clusternet/clusternet/pkg/utils"
)

const (
	// NameTopologyAssigner is the name of the plugin used in the plugin registry and configurations.
	NameTopologyAssigner = names.TopologyAssigner
)

// TopologyAssigner assigns replicas to clusters.
type TopologyAssigner struct {
	handle framework.Handle
}

var _ framework.AssignPlugin = &TopologyAssigner{}

// NewTopologyAssigner creates a topology assigner.
func NewTopologyAssigner(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &TopologyAssigner{handle: handle}, nil
}

// Name returns the name of the plugin.
func (pl *TopologyAssigner) Name() string {
	return NameTopologyAssigner
}

// Assign assigns subscriptions to clusters using the clusternet client.
// clusterScoreList framework.ClusterScoreList
func (pl *TopologyAssigner) Assign(ctx context.Context,
	state *framework.CycleState,
	sub *appsapi.Subscription,
	finv *appsapi.FeedInventory,
	availableReplicas framework.TargetClusters) (framework.TargetClusters, *framework.Status) {

	klog.V(5).InfoS("Attempting to assign replicas to clusters",
		"subscription", klog.KObj(sub), "clusters",
		availableReplicas.BindingClusters,
		"replicas", availableReplicas.Replicas,
		"topology", availableReplicas.TopologyReplicas)

	if !metav1.HasAnnotation(sub.ObjectMeta, known.AnnoEnableTopology) || sub.Spec.DividingScheduling == nil {
		klog.V(5).Info("not enable dividing or topology scheduling, skip")
		return framework.TargetClusters{}, framework.NewStatus(framework.Skip, "")
	}

	var result framework.TargetClusters

	switch sub.Spec.DividingScheduling.Type {
	case appsapi.StaticReplicaDividingType:
		klog.V(5).Infof("sub %s/%s will run static topology scheduling ... ", sub.Namespace, sub.Name)
		if metav1.HasAnnotation(sub.ObjectMeta, known.AnnoTopologyReplicasResult) {
			data := sub.ObjectMeta.Annotations[known.AnnoTopologyReplicasResult]
			err := json.Unmarshal([]byte(data), &result)
			if err != nil {
				klog.Warning("error of result annotation data : %s", data)
			} else {
				return result, nil
			}
		}
		result, err := StaticTopologyDivideReplicas(sub, finv, &availableReplicas)
		if err != nil {
			return framework.TargetClusters{}, framework.AsStatus(err)
		}
		return result, nil

	case appsapi.DynamicReplicaDividingType:
		// TODO: Currently dynamic topology schedule use default algorithm
		// Because of available replicas has been processed in score plugin
		// I will implement an algoritnm same with static topology schedule as soon as possiable
		klog.V(5).Infof("sub %s/%s will run dynamic topology scheduling ... ", sub.Namespace, sub.Name)
		s, err := getPreAssignState(state)
		if err != nil {
			return framework.TargetClusters{}, framework.AsStatus(err)
		}
		availableReplicas, err := getAvailableReplicas(state)
		if err != nil {
			return framework.TargetClusters{}, framework.AsStatus(err)
		}
		klog.V(5).Infof("subs %s/%s will use available replicas %v from pre-assigne because enable dynamic topology schedule,",
			sub.Namespace, sub.Name, availableReplicas)
		result, err := DynamicTopologyDivideReplicas(sub, finv, s.Deviations, availableReplicas)
		if err != nil {
			return framework.TargetClusters{}, framework.AsStatus(err)
		}
		return result, nil

	default:
		klog.V(5).Info("not set topology scheduling type , skip ... ")
		return framework.TargetClusters{}, framework.NewStatus(framework.Skip, "")
	}
}

func StaticTopologyDivideReplicas(sub *appsapi.Subscription,
	feeds *appsapi.FeedInventory,
	availableReplicas *framework.TargetClusters,
) (framework.TargetClusters, error) {
	var err error
	var result framework.TargetClusters
	for _, feed := range feeds.Spec.Feeds {
		var r framework.TargetClusters
		r.Replicas = make(map[string][]int32)
		if feed.DesiredReplicas != nil {
			// feed is a workload
			r, err = topologyReplicasOneFeed(sub, &feed, availableReplicas)
			if err != nil {
				klog.Errorf("subs %s/%s topology replicas with feed %s error : %v",
					sub.Namespace, sub.Name, feed.Name, err)
				return framework.TargetClusters{}, err
			}
		} else {
			// feed is not a workload, set replicas is nil
			r.Replicas[utils.GetFeedKey(feed.Feed)] = []int32{}
		}
		// merge feed scheduling result r into source result
		result.MergeOneFeedWithTopology(&r, false)
	}
	return result, err
}
