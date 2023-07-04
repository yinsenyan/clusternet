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
	"encoding/json"
	"fmt"
	"math"
	"sort"

	"k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/defaultassigner"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/names"
	"github.com/clusternet/clusternet/pkg/utils"
)

const (
	// preAssignStateKey is the key in CycleState to DynamicAssigner pre-computed data.
	// Using the name of the plugin will likely help us avoid collisions with other plugins.
	preAssignStateKey = "PreAssign" + names.DynamicAssigner
)

func getPreAssignState(cycleState *framework.CycleState) (*defaultassigner.PreAssignState, error) {
	c, err := cycleState.Read(preAssignStateKey)
	if err != nil {
		// preAssignState doesn't exist, likely PreAssign wasn't invoked.
		return nil, fmt.Errorf("error reading %q from cycleState: %w", preAssignStateKey, err)
	}

	s, ok := c.(*defaultassigner.PreAssignState)
	if !ok {
		return nil, fmt.Errorf("%+v convert to DynamicAssigner.preAssignState error", c)
	}
	return s, nil
}

// DynamicDivideReplicas will fill the target replicas of all feeds based on predictor result and deviation.
// First time scheduling is considered as a special kind of scaling. When the desired replicas in deviation
// are negative, it means we should try to scale down, otherwise we try to scale up deviation replicas.
func DynamicTopologyDivideReplicas(sub *appsapi.Subscription, finv *appsapi.FeedInventory, deviations []appsapi.FeedOrder, availableReplicas framework.TargetClusters) (framework.TargetClusters, error) {
	var err error

	var result framework.TargetClusters
	topologyData, isok := sub.Annotations[known.AnnoTopologyReplicas]
	if !isok {
		klog.V(5).Infof("sub %s/%s has not topology data in annotation, is first schedule...",
			sub.Namespace, sub.Name)
	} else {
		err = json.Unmarshal([]byte(topologyData), &result)
		if err != nil {
			klog.Warningf("unmarshal subs %s/%s topology data %s from annotation error: %v", sub.Namespace, sub.Name, topologyData, err)
			return result, fmt.Errorf("unmarshal subs %s/%s topology data %s from annotation error: %v", sub.Namespace, sub.Name, topologyData, err)
		}
	}

	strategy := sub.Spec.DividingScheduling.DynamicDividing.Strategy

	var add bool = true
	for i := range deviations {
		feedKey := utils.GetFeedKey(deviations[i].Feed)
		d := deviations[i].DesiredReplicas
		_, scheduled := sub.Status.Replicas[utils.GetFeedKey(deviations[i].Feed)]
		var r framework.TargetClusters

		switch {
		// First scheduling is considered as a special kind of scaling up.
		case !scheduled || (d != nil && *d > 0):
			// TODO implement all strategy algorithm
			switch strategy {
			case appsapi.AverageDividingStrategy, appsapi.CapacityDividingStrategy:
				r = spreadScaleUp(&deviations[i], availableReplicas)
			case appsapi.SpreadDividingStrategy, appsapi.BinpackDividingStrategy:
				r = spreadScaleUp(&deviations[i], availableReplicas)
			}
		case d != nil && *d < 0:
			switch strategy {
			case appsapi.AverageDividingStrategy, appsapi.CapacityDividingStrategy:
				r = spreadScaleDown(sub, deviations[i])
			case appsapi.SpreadDividingStrategy, appsapi.BinpackDividingStrategy:
				r = spreadScaleDown(sub, deviations[i])
			}
		case len(sub.Status.BindingClusters) != len(availableReplicas.BindingClusters) && d != nil:
			// if re-schedule topology domains, like first schedule, but use current cluster in pre-assign
			klog.V(5).Infof("subscription %s/%s feed %s use new domains number",
				sub.Namespace, sub.Name, utils.GetFeedKey(finv.Spec.Feeds[i].Feed))
			r = spreadScaleUp(&finv.Spec.Feeds[i], availableReplicas)
			add = false
		case d == nil:
			klog.Infof("sub %s/%s feed %s not workload use nil replicas ",
				sub.Namespace, sub.Name, utils.GetFeedKey(deviations[i].Feed))
			r = framework.TargetClusters{
				BindingClusters: []string{},
				Replicas:        map[string][]int32{utils.GetFeedKey(deviations[i].Feed): {}},
			}
		case *d == 0:
			klog.V(5).Infof("sub %s/%s feed %s desired replica is 0, skip schedule ... ",
				sub.Namespace, sub.Name, feedKey)
		}

		klog.V(5).Infof("Subscription %s/%s feed %s schedule result is %v, available is %v",
			sub.Namespace, sub.Name, feedKey, r, availableReplicas)
		// set workload cluster topology replicas
		for index, cluster := range r.BindingClusters {
			if len(r.Replicas[feedKey]) == 0 {
				continue
			}
			var topologyReplicas []framework.TopologyReplica
			i1 := utils.GetIndex(availableReplicas.BindingClusters, cluster)
			if i1 == -1 {
				klog.V(5).Infof("Subscription %s/%s feed %s schedule result cluster %s not in available is %v, try use current cluster %v",
					sub.Namespace, sub.Name, feedKey, cluster, availableReplicas, result)
				i2 := utils.GetIndex(result.BindingClusters, cluster)
				if i2 == -1 {
					klog.V(5).Infof("Subscription %s/%s feed %s schedule result cluster %s not in result %v, use 0",
						sub.Namespace, sub.Name, feedKey, cluster, availableReplicas)
				} else {
					topologyReplicas = result.TopologyReplicas[i2].FeedReplicas[feedKey]
				}
			} else {
				topologyReplicas = availableReplicas.TopologyReplicas[i1].FeedReplicas[feedKey]
			}
			if len(topologyReplicas) == 0 {
				topologyReplicas = append(topologyReplicas, framework.TopologyReplica{
					Topology: map[string]string{}, Replica: 0,
				})
			}
			domain := topologyReplicas[0].Topology
			r.TopologyReplicas = append(r.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					feedKey: {{Topology: domain, Replica: r.Replicas[feedKey][index]}},
				},
			})
		}

		result.MergeOneFeedWithTopology(&r, add)
	}
	return result, nil
}

func spreadScaleUp(d *appsapi.FeedOrder, availableReplicas framework.TargetClusters) framework.TargetClusters {
	result := framework.TargetClusters{
		BindingClusters: availableReplicas.BindingClusters,
		Replicas:        make(map[string][]int32),
	}
	feedKey := utils.GetFeedKey(d.Feed)
	if d.DesiredReplicas != nil {
		replicas := dynamicDivideReplicas(*d.DesiredReplicas, availableReplicas.Replicas[feedKey])
		result.Replicas[feedKey] = replicas
	} else {
		result.Replicas[feedKey] = []int32{}
	}
	return result
}

func spreadScaleDown(sub *appsapi.Subscription, d appsapi.FeedOrder) framework.TargetClusters {
	result := framework.TargetClusters{
		BindingClusters: sub.Status.BindingClusters,
		Replicas:        make(map[string][]int32),
	}
	feedKey := utils.GetFeedKey(d.Feed)
	if d.DesiredReplicas != nil {
		replicas := dynamicDivideReplicas(*d.DesiredReplicas, sub.Status.Replicas[feedKey])
		result.Replicas[feedKey] = replicas
	} else {
		result.Replicas[feedKey] = []int32{}
	}
	return result
}

// dynamicDivideReplicas divides replicas by the MaxAvailableReplicas
func dynamicDivideReplicas(desiredReplicas int32, maxAvailableReplicas []int32) []int32 {
	res := make([]int32, len(maxAvailableReplicas))
	weightSum := utils.SumArrayInt32(maxAvailableReplicas)

	if weightSum == 0 || desiredReplicas > weightSum {
		return maxAvailableReplicas
	}

	remain := desiredReplicas

	type cluster struct {
		index   int
		decimal float64
	}
	clusters := make([]cluster, 0, len(maxAvailableReplicas))

	for i := range maxAvailableReplicas {
		replica := desiredReplicas / int32(len(maxAvailableReplicas))
		res[i] = replica
		remain -= replica
		clusters = append(clusters, cluster{
			index:   i,
			decimal: math.Abs(float64(desiredReplicas)/float64(len(maxAvailableReplicas)) - float64(replica)),
		})
	}

	// sort the clusters by descending order of decimal part of replica
	sort.Slice(clusters, func(i, j int) bool {
		return clusters[i].decimal > clusters[j].decimal
	})

	if remain > 0 {
		for i := 0; i < int(remain) && i < len(res); i++ {
			res[clusters[i].index]++
		}
	} else if remain < 0 {
		for i := 0; i < int(-remain) && i < len(res); i++ {
			res[clusters[i].index]--
		}
	}

	return res
}
