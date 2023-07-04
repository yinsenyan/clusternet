package topologyassigner

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	klog "k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/names"
	"github.com/clusternet/clusternet/pkg/utils"
)

const (
	DynamicTopologyPreAssignKey = "PreAssign" + names.TopologyAssigner
)

type TopologyPreAssignState struct {
	TargetClusters framework.TargetClusters
}

func (s *TopologyPreAssignState) Clone() framework.StateData {
	return s
}

var _ framework.PreAssignPlugin = &TopologyAssigner{}

func (pl *TopologyAssigner) PreAssign(ctx context.Context,
	state *framework.CycleState,
	sub *appsapi.Subscription,
	finv *appsapi.FeedInventory,
	availableReplicas framework.TargetClusters) *framework.Status {

	klog.V(5).InfoS("Attempting to run pre-assign plugin",
		"subscription", klog.KObj(sub), "clusters",
		availableReplicas.BindingClusters,
		"replicas", availableReplicas.Replicas,
		"topology", availableReplicas.TopologyReplicas)

	if sub.Spec.DividingScheduling == nil || sub.Spec.DividingScheduling.Type != appsapi.DynamicReplicaDividingType {
		klog.V(5).Infof("sub %s/%s will skip dynamic topology pre-assigner %s because of scheduling type not match ", sub.Namespace, sub.Name, pl.Name())
		return framework.NewStatus(framework.Skip, "")
	}
	if !metav1.HasAnnotation(sub.ObjectMeta, known.AnnoEnableTopology) {
		klog.V(5).Infof("sub %s/%s will skip dynamic topology pre-assigner %s because enable topology scheduling", sub.Namespace, sub.Name, pl.Name())
		return framework.NewStatus(framework.Skip, "")
	}

	var result, current framework.TargetClusters

	min, max := getZoneRange(sub, availableReplicas)

	var data TopologyPreAssignState
	if metav1.HasAnnotation(sub.ObjectMeta, known.AnnoTopologyReplicas) {
		data := sub.ObjectMeta.Annotations[known.AnnoTopologyReplicas]
		err := json.Unmarshal([]byte(data), &current)
		if err != nil {
			klog.Warningf("unmarshal data from subs %s/%s annotation , use nil to schedule, err is : %v", sub.Namespace, sub.Name, err)
		}
	}

	var domains []map[string]string
	for _, s := range sub.Spec.Subscribers {
		domain := make(map[string]string)
		for k, v := range s.ClusterAffinity.MatchLabels {
			if utils.IsZoneKey(k) {
				domain[k] = v
			}
		}
		if len(domain) > 0 {
			domains = append(domains, domain)
		}
	}

	for _, feed := range finv.Spec.Feeds {
		if feed.DesiredReplicas == nil {
			continue
		}

		for i := range current.BindingClusters {
			if len(current.TopologyReplicas) < i ||
				len(current.TopologyReplicas[i].FeedReplicas) < 1 ||
				len(current.TopologyReplicas[i].FeedReplicas[utils.GetFeedKey(feed.Feed)]) < 1 {
				klog.Warningf("sub %s/%s topology replicas data in annotation format error, use nil to schedule like first schedule",
					sub.Namespace, sub.Name)
				current = framework.TargetClusters{}
				break
				//return framework.AsStatus(fmt.Errorf("annotataion topology replicas data error"))
			}
		}
		var r *framework.TargetClusters
		if len(domains) != 0 {
			r = findTopologyClustersWithDomain(availableReplicas, current, feed, domains)
		} else {
			r = findTopologyClusters(availableReplicas, current, feed, max, min)
		}
		result.MergeOneFeedWithTopology(r, false)
	}

	data.TargetClusters = result
	state.Write(DynamicTopologyPreAssignKey, &data)
	return nil
}

func getZoneRange(sub *appsapi.Subscription, availableReplicas framework.TargetClusters) (min, max int) {
	maxStr, isok := sub.Annotations[known.AnnoTopologyMaxZones]
	max, err := strconv.Atoi(maxStr)
	if !isok || err != nil {
		max = len(availableReplicas.GetDomains())
		klog.Warningf("sub %s/%s max zones annotation value invide %v or have not this annotation , use current value %d", sub.Namespace, sub.Name, err, max)
	}

	minStr, isok := sub.Annotations[known.AnnoTopologyMinZones]
	min, err = strconv.Atoi(minStr)
	if !isok || err != nil {
		klog.Warningf("sub %s/%s min zones annotation value invide %v or have not this annotation , use default value 2", sub.Namespace, sub.Name, err)
		min = 2
	}

	if max < min {
		max = min
		klog.Warningf("sub %s/%s max zones %d less then min zones %d, use min", sub.Name, sub.Namespace, max, min)
	}

	return
}

func findTopologyClustersWithDomain(availableReplicas, current framework.TargetClusters, feed appsapi.FeedOrder, domains []map[string]string) *framework.TargetClusters {
	feedKey := utils.GetFeedKey(feed.Feed)
	var result framework.TargetClusters
	var resultReplicas = map[string][]int32{feedKey: {}}
	var maxReplicas int32

	var currentDomains []map[string]string
	for _, v := range current.TopologyReplicas {
		currentDomains = append(currentDomains, v.FeedReplicas[feedKey][0].Topology)
	}
	for _, domain := range domains {
		var maxReplica int32
		var clusterID *string
		i := utils.GetMapsIndex(currentDomains, domain)
		if i != -1 {
			// find existing cluster
			maxReplica, clusterID = findExistCluster(&availableReplicas, current.BindingClusters[i], feedKey, domain)
			if maxReplica > *feed.DesiredReplicas/int32(len(domains)) {
				result.BindingClusters = append(result.BindingClusters, *clusterID)
				resultReplicas[feedKey] = append(resultReplicas[feedKey], maxReplica)
				result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: maxReplica}},
					},
				})
				maxReplicas += maxReplica
				continue
			}
		}
		// find new cluster
		for i := 0; i < len(availableReplicas.BindingClusters); i++ {
			maxReplica, clusterID = findNewCluster(&availableReplicas, result.BindingClusters, feedKey, domain)
			if clusterID == nil {
				continue
			}
			result.BindingClusters = append(result.BindingClusters, *clusterID)
			resultReplicas[feedKey] = append(resultReplicas[feedKey], maxReplica)
			result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					feedKey: {{Topology: domain, Replica: maxReplica}},
				},
			})
			maxReplicas += maxReplica

			if maxReplicas > *feed.DesiredReplicas && len(result.BindingClusters) > len(domains) {
				break
			}

		}
	}
	/*
		// add existing cluster to result
		for index, clusterReplicas := range current.TopologyReplicas {
			if !utils.ContainsString(availableReplicas.BindingClusters, current.BindingClusters[index]) {
				continue
			}

			domain := clusterReplicas.FeedReplicas[feedKey][0].Topology
			i := utils.GetIndex(availableReplicas.BindingClusters, current.BindingClusters[index])
			maxReplica := availableReplicas.TopologyReplicas[i].GetReplica(feedKey, domain)
			if maxReplica != 0 {
				maxReplicas += maxReplica
				result.BindingClusters = append(result.BindingClusters, current.BindingClusters[index])
				resultReplicas[feedKey] = append(resultReplicas[feedKey], maxReplica)
				result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{
							Topology: domain,
							Replica:  maxReplica,
						}},
					},
				})
			}
			domains = utils.RemoveMap(domains, domain)
		}
		result.Replicas = resultReplicas

		// stop search cluster when all domain has replicas and replica sum more then desire replica
		if len(domains) == 0 && maxReplicas > *feed.DesiredReplicas {
			return &result
		}

		// add new cluster for new domain or need more replicas
		for index, clusterReplicas := range availableReplicas.TopologyReplicas {
			for _, topology := range clusterReplicas.FeedReplicas[feedKey] {
				if utils.ContainesMap(domains, topology.Topology) {
					if !utils.ContainsString(result.BindingClusters, availableReplicas.BindingClusters[index]) {
						result.BindingClusters = append(result.BindingClusters, availableReplicas.BindingClusters[index])
						resultReplicas[feedKey] = append(resultReplicas[feedKey], clusterReplicas.GetReplica(feedKey, topology.Topology))
						result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
							FeedReplicas: map[string][]framework.TopologyReplica{
								feedKey: {topology},
							},
						})
						maxReplicas += topology.Replica
					} else {
						i := utils.GetIndex(result.BindingClusters, availableReplicas.BindingClusters[index])
						resultReplicas[feedKey][i] += topology.Replica
						result.TopologyReplicas[i].FeedReplicas[feedKey] = append(result.TopologyReplicas[i].FeedReplicas[feedKey], topology)
						maxReplicas += topology.Replica
					}
				}
			}
			// stop search cluster when enough domain and replicas
			if index > len(domains) && maxReplicas > *feed.DesiredReplicas {
				break
			}
		}
	*/
	result.Replicas = resultReplicas
	return &result
}

func findTopologyClusters(availableReplicas, current framework.TargetClusters, feed appsapi.FeedOrder, max, min int) *framework.TargetClusters {
	feedKey := utils.GetFeedKey(feed.Feed)
	var result framework.TargetClusters
	var resultReplicas = map[string][]int32{feedKey: {}}
	var maxReplicas int32
	for i := 0; i < max; i++ {
		var maxReplica int32
		var clusterID *string
		if len(current.BindingClusters) > i {
			domain := current.TopologyReplicas[i].FeedReplicas[feedKey][0].Topology
			maxReplica, clusterID = findExistCluster(&availableReplicas, current.BindingClusters[i], feedKey, domain)
			if clusterID == nil {
				// current cluster not available replica , find new with same topology
				maxReplica, clusterID = findNewCluster(&availableReplicas, current.BindingClusters, feedKey, domain)
				if clusterID == nil || maxReplica == 0 {
					continue
				}
			}
			maxReplicas += maxReplica
			result.BindingClusters = append(result.BindingClusters, *clusterID)
			resultReplicas[feedKey] = append(resultReplicas[feedKey], maxReplica)
			result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					feedKey: {{Topology: domain, Replica: maxReplica}},
				},
			})
			result.Score = append(result.Score, availableReplicas.Score[i])
		} else {
			for index, cluster := range availableReplicas.BindingClusters {
				feedReplicas := availableReplicas.TopologyReplicas[index].FeedReplicas[feedKey]
				// skip cluster which have not feed replicas or has been selected
				if len(feedReplicas) == 0 || utils.ContainsString(result.BindingClusters, availableReplicas.BindingClusters[index]) {
					continue
				}
				// skip cluster which topology data invide
				if reflect.DeepEqual(feedReplicas[0].Topology, map[string]string{}) {
					continue
				}
				sort.Slice(feedReplicas, func(i, j int) bool {
					return feedReplicas[i].Replica > feedReplicas[j].Replica
				})
				var existDomains []map[string]string
				for _, v := range result.TopologyReplicas {
					existDomains = append(existDomains, v.FeedReplicas[feedKey][0].Topology)
				}
				domain, maxReplica := findDomainInCluster(existDomains, feedReplicas, *feed.DesiredReplicas/int32(i+1))
				if domain == nil {
					continue
				}

				result.BindingClusters = append(result.BindingClusters, cluster)
				resultReplicas[feedKey] = append(resultReplicas[feedKey], maxReplica)
				result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: maxReplica}},
					},
				})
				result.Score = append(result.Score, availableReplicas.Score[index])
				maxReplicas += maxReplica
				break
			}
		}

		// stop searching when replicas sum more then desire replica, and zone count reach target
		if i >= min-1 && maxReplicas > *feed.DesiredReplicas {
			break
		}

		// break when available replica cluster less then i
		if i > len(availableReplicas.BindingClusters) {
			break
		}
	}
	result.Replicas = resultReplicas
	return &result
}

func findDomainInCluster(existDomain []map[string]string, feedReplicas []framework.TopologyReplica, averageReplica int32) (map[string]string, int32) {
	for _, f := range feedReplicas {
		if utils.ContainesMap(existDomain, f.Topology) {
			continue
		}
		if f.Replica < averageReplica {
			continue
		}
		return f.Topology, f.Replica
	}
	return nil, 0
}

func getAvailableReplicas(cycleState *framework.CycleState) (framework.TargetClusters, error) {
	c, err := cycleState.Read(DynamicTopologyPreAssignKey)
	if err != nil {
		return framework.TargetClusters{}, nil
	}
	s, ok := c.(*TopologyPreAssignState)
	if !ok {
		return framework.TargetClusters{}, fmt.Errorf("")
	}
	return s.TargetClusters, nil
}
