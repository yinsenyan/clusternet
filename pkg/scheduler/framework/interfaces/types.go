/*
Copyright 2021 The Clusternet Authors.

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

package interfaces

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/utils"
)

// Diagnosis records the details to diagnose a scheduling failure.
type Diagnosis struct {
	ClusterToStatusMap   ClusterToStatusMap
	UnschedulablePlugins sets.Set[string]
}

// FitError describes a fit error of a subscription.
type FitError struct {
	Subscription   *appsapi.Subscription
	NumAllClusters int
	Diagnosis      Diagnosis
}

// TargetClusters represents the scheduling result.
type TargetClusters struct {
	// Namespaced names of targeted clusters that Subscription binds to.
	BindingClusters []string `json:"bindingClusters"`

	// Desired replicas of targeted clusters for each feed. map key is feed.
	Replicas map[string][]int32 `json:"replicas"`

	// Topology desired replicas of targeted clusters for each feed.
	// array align to bindingclusters
	TopologyReplicas []ClusterReplicas `json:"topologyReplicas"`

	// Score is cluster score
	Score []int64 `json:"score"`
}

// ClusterReplicas is cluster replicas with all feed and topology
type ClusterReplicas struct {
	// map key string is feed key
	// array topologyReplica is multi topology in one cluster
	FeedReplicas map[string][]TopologyReplica `json:"feedReplicas"`
}

// TopologyReplicas is topology replica with topology
type TopologyReplica struct {
	Topology map[string]string `json:"topology"`
	Replica  int32             `json:"replica"`
}

func (tr *TargetClusters) GetReplicas(feedKey string, domain map[string]string) []int32 {
	if tr == nil || len(tr.BindingClusters) == 0 {
		return make([]int32, 0)
	}

	if tr.TopologyReplicas == nil {
		return tr.Replicas[feedKey]
	}

	var currentReplicas = make([]int32, len(tr.BindingClusters))
	for i, clsReplica := range tr.TopologyReplicas {
		for _, topologyReplica := range clsReplica.FeedReplicas[feedKey] {
			if reflect.DeepEqual(domain, topologyReplica.Topology) {
				currentReplicas[i] = topologyReplica.Replica
			}
		}
	}

	return currentReplicas
}

func (tr *TargetClusters) GetDomains() []map[string]string {
	var result []map[string]string
	for _, clusterReplicas := range tr.TopologyReplicas {
		for _, topologyReplicas := range clusterReplicas.FeedReplicas {
			for _, topology := range topologyReplicas {
				if !utils.ContainesMap(result, topology.Topology) {
					result = append(result, topology.Topology)
				}
			}
		}
	}
	return result
}

func TopologyReplicasSum(replicas []TopologyReplica) (result int32) {
	for _, r := range replicas {
		result += r.Replica
	}
	return
}

func MergeTwoTopologyReplicas(target, source []TopologyReplica, add bool) []TopologyReplica {
	if len(source) == 0 {
		return target
	}
	if len(target) == 0 {
		return source
	}

	merged := make([]TopologyReplica, 0, len(target)+len(source))
	for _, sourceReplica := range source {
		mergedOne := sourceReplica
		for _, targetReplica := range target {
			if reflect.DeepEqual(sourceReplica.Topology, targetReplica.Topology) {
				if add {
					mergedOne.Replica += targetReplica.Replica
				} else {
					mergedOne.Replica = sourceReplica.Replica
				}
			}
		}
		merged = append(merged, mergedOne)
	}
	for _, targetReplica := range target {
		found := false
		for _, sourceReplica := range source {
			if reflect.DeepEqual(sourceReplica.Topology, targetReplica.Topology) {
				found = true
				break
			}
		}
		if !found {
			merged = append(merged, targetReplica)
		}
	}

	return merged
}

func (c *ClusterReplicas) SumReplica(feed string) int32 {
	var sum int32 = 0
	for _, t := range c.FeedReplicas[feed] {
		sum += t.Replica
	}
	return sum
}

func (c *ClusterReplicas) GetReplica(feed string, domain map[string]string) int32 {
	for _, v := range c.FeedReplicas[feed] {
		if reflect.DeepEqual(domain, v.Topology) {
			return v.Replica
		}
	}
	return 0
}

func (c *ClusterReplicas) SetReplica(feed string, domain map[string]string, replica int32) bool {
	for i, t := range c.FeedReplicas[feed] {
		if reflect.DeepEqual(t.Topology, domain) {
			c.FeedReplicas[feed][i].Replica = replica
			return true
		}
	}
	return false
}

func (c *ClusterReplicas) AddReplica(feed string, domain map[string]string, replica int32) {
	for i, t := range c.FeedReplicas[feed] {
		if reflect.DeepEqual(t.Topology, domain) {
			c.FeedReplicas[feed][i].Replica += replica
		}
	}
}

func NewTargetClusters(bindingClusters []string, replicas map[string][]int32) *TargetClusters {
	return &TargetClusters{
		BindingClusters:  bindingClusters,
		Replicas:         replicas,
		TopologyReplicas: make([]ClusterReplicas, len(bindingClusters)),
	}
}

func (t TargetClusters) Len() int {
	return len(t.BindingClusters)
}

func (t TargetClusters) Less(i, j int) bool {
	if len(t.Score) > i && len(t.Score) > j {
		return t.Score[i] < t.Score[j]
	}
	return t.BindingClusters[i] < t.BindingClusters[j]
}

func (t TargetClusters) Swap(i, j int) {
	t.BindingClusters[i], t.BindingClusters[j] = t.BindingClusters[j], t.BindingClusters[i]
	for _, replicas := range t.Replicas {
		if len(replicas) == len(t.BindingClusters) {
			replicas[i], replicas[j] = replicas[j], replicas[i]
		}
	}
	if len(t.TopologyReplicas) > i && len(t.TopologyReplicas) > j {
		t.TopologyReplicas[i], t.TopologyReplicas[j] = t.TopologyReplicas[j], t.TopologyReplicas[i]
	}
}

func (t *TargetClusters) DeepCopy() *TargetClusters {
	if t == nil {
		return nil
	}
	obj := new(TargetClusters)
	if t.BindingClusters != nil {
		in, out := &t.BindingClusters, &obj.BindingClusters
		*out = make([]string, len(*in))
		copy(*out, *in)
	}
	if t.Replicas != nil {
		in, out := &t.Replicas, &obj.Replicas
		*out = make(map[string][]int32, len(*in))
		for key, val := range *in {
			var outVal []int32
			if val == nil {
				(*out)[key] = nil
			} else {
				in, out := &val, &outVal
				*out = make([]int32, len(*in))
				copy(*out, *in)
			}
			(*out)[key] = outVal
		}
	}
	if t.TopologyReplicas != nil {
		in, out := &t.TopologyReplicas, &obj.TopologyReplicas
		*out = make([]ClusterReplicas, len(*in))
		for key, val := range *in {
			var outVal ClusterReplicas
			if val.FeedReplicas == nil {
				(*out)[key] = ClusterReplicas{make(map[string][]TopologyReplica, 0)}
			} else {
				in, out := &val, &outVal
				*out = ClusterReplicas{
					make(map[string][]TopologyReplica, len((*in).FeedReplicas)),
				}
				for k, v := range in.FeedReplicas {
					out.FeedReplicas[k] = v
				}
			}
			(*out)[key] = outVal
		}
	}
	return obj
}

func (t *TargetClusters) DeleteByIndex(i int) {
	var result TargetClusters
	for index, cluster := range t.BindingClusters {
		if index == i {
			continue
		}
		result.BindingClusters = append(result.BindingClusters, cluster)
		result.TopologyReplicas = append(result.TopologyReplicas, t.TopologyReplicas[index])
	}

	resultFeed := make(map[string][]int32)
	for feed, replicas := range t.Replicas {
		var resultReplicas []int32
		for index, replica := range replicas {
			if index == i {
				continue
			}
			resultReplicas = append(resultReplicas, replica)
		}
		resultFeed[feed] = resultReplicas
	}
	t.BindingClusters = result.BindingClusters
	t.TopologyReplicas = result.TopologyReplicas
	t.Replicas = resultFeed
}

// MergeOneFeedWithTopology will merge two TargetCluster
// Merge b into t
// If add set true , t topology replica will sum b topology replica with same feed&cluster&domain
// If add set false, b topology replica will replace t topology replica with same feed&cluster&domain
func (t *TargetClusters) MergeOneFeedWithTopology(b *TargetClusters, add bool) {
	klog.V(5).Infof("merge one feed with topology , t is : %v, b is %v", t, b)
	if b == nil || len(b.Replicas) > 1 {
		klog.Warningf("merge assgin result %v is nil or more then one feed", b)
		return
	}
	if t.Replicas == nil {
		t.Replicas = make(map[string][]int32)
	}
	// If the feed replicas does not exist in the former target clusters, we initialize them as an empty one.
	for feed, replicas := range b.Replicas {
		if _, exist := t.Replicas[feed]; exist {
			continue
		}
		if len(replicas) == 0 {
			t.Replicas[feed] = make([]int32, 0)
		} else {
			t.Replicas[feed] = make([]int32, len(t.BindingClusters))
		}
	}

	// transfer from cluster to index
	m := make(map[string]int)
	for i, cluster := range t.BindingClusters {
		m[cluster] = i
	}
	// only one feed in b.Replicas, get feed and replicas by loop
	for feed, replicas := range b.Replicas {
		if len(replicas) == 0 || len(b.TopologyReplicas) == 0 {
			if _, isok := t.Replicas[feed]; !isok {
				t.Replicas[feed] = make([]int32, 0)
			}
			return
		}

		for bi, bcluster := range b.BindingClusters {
			if add && replicas[bi] == 0 {
				// if use add model, b replica is 0, skip add
				continue
			}
			if ti, isok := m[bcluster]; isok {
				if t.TopologyReplicas[ti].FeedReplicas == nil {
					t.TopologyReplicas[ti].FeedReplicas = map[string][]TopologyReplica{
						feed: []TopologyReplica{},
					}
				}
				t.TopologyReplicas[ti].FeedReplicas[feed] = MergeTwoTopologyReplicas(t.TopologyReplicas[ti].FeedReplicas[feed],
					b.TopologyReplicas[bi].FeedReplicas[feed], add)
				t.Replicas[feed][ti] = t.TopologyReplicas[ti].SumReplica(feed)
			} else {
				t.BindingClusters = append(t.BindingClusters, bcluster)
				t.Replicas[feed] = append(t.Replicas[feed], b.Replicas[feed][bi])
				t.TopologyReplicas = append(t.TopologyReplicas, ClusterReplicas{
					FeedReplicas: b.TopologyReplicas[bi].FeedReplicas,
				})
			}
		}
	}
	// delete cluster which all feed replica is 0 in descending order
	replicas := utils.ClusterReplicaSum(t.Replicas)
	for i := len(replicas) - 1; i >= 0; i-- {
		if replicas[i] == 0 {
			t.DeleteByIndex(i)
		}
	}
}

// If b.Replicas have feed more then one , use this method, it works good
// https://github.com/clusternet/clusternet/blob/v0.14.0/pkg/scheduler/framework/interfaces/types.go#L100
// MergeOneFeed use for merge two TargetClusters when b.Replicas only one feed
func (t *TargetClusters) MergeOneFeed(b *TargetClusters) {
	if b == nil || len(b.BindingClusters) == 0 {
		return
	}
	if t.Replicas == nil {
		t.Replicas = make(map[string][]int32)
	}
	// If the feed replicas does not exist in the former target clusters, we initialize them as an empty one.
	for feed, replicas := range b.Replicas {
		if _, exist := t.Replicas[feed]; exist {
			continue
		}
		if len(replicas) == 0 {
			t.Replicas[feed] = make([]int32, 0)
		} else {
			t.Replicas[feed] = make([]int32, len(t.BindingClusters))
		}
	}

	// transfer from cluster to index
	m := make(map[string]int)
	for i, cluster := range t.BindingClusters {
		m[cluster] = i
	}

	// only one feed in b.Replicas, get feed and replicas by loop
	for feed, replicas := range b.Replicas {
		if len(replicas) == 0 {
			if _, isok := t.Replicas[feed]; !isok {
				t.Replicas[feed] = make([]int32, 0)
			}
			return
		}
		for bi, cluster := range b.BindingClusters {
			// same cluster , use b binding cluster index get replica, and assign to t replicas
			if ti, exist := m[cluster]; exist {
				if len(t.Replicas[feed]) != len(t.BindingClusters) {
					t.Replicas[feed] = make([]int32, len(t.BindingClusters))
					t.Replicas[feed][ti] = replicas[bi]
					continue
				}
				t.Replicas[feed][ti] += replicas[bi]
			} else {
				// new cluster, add cluster to t binding cluster
				t.BindingClusters = append(t.BindingClusters, cluster)
				// add 0 to exist feed but new cluster, if feed is nil, skip it
				for f := range t.Replicas {
					if f != feed {
						if len(t.Replicas[f]) != 0 {
							t.Replicas[f] = append(t.Replicas[f], 0)
						}
					} else {
						t.Replicas[f] = append(t.Replicas[f], b.Replicas[feed][bi])
					}
				}
			}
		}
	}
}

const (
	// NoClusterAvailableMsg is used to format message when no clusters available.
	NoClusterAvailableMsg = "0/%v clusters are available"
)

// Error returns detailed information of why the subscription failed to fit on each cluster
func (f *FitError) Error() string {
	reasons := make(map[string]int)
	for _, status := range f.Diagnosis.ClusterToStatusMap {
		for _, reason := range status.Reasons() {
			reasons[reason]++
		}
	}

	sortReasonsHistogram := func() []string {
		var reasonStrings []string
		for k, v := range reasons {
			reasonStrings = append(reasonStrings, fmt.Sprintf("%v %v", v, k))
		}
		sort.Strings(reasonStrings)
		return reasonStrings
	}
	reasonMsg := fmt.Sprintf(NoClusterAvailableMsg+": %v.", f.NumAllClusters, strings.Join(sortReasonsHistogram(), ", "))
	return reasonMsg
}
