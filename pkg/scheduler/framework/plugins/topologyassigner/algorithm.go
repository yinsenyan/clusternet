package topologyassigner

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"

	klog "k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/utils"
)

type TopologyReplicas struct {
	TargetClusters    *framework.TargetClusters
	AvailableReplicas *framework.TargetClusters
	FeedKey           string
	WeightSum         int32
}

func NewTopologyReplicas(sub *appsapi.Subscription, feed *appsapi.FeedOrder, availableReplicas *framework.TargetClusters) *TopologyReplicas {
	var result = &TopologyReplicas{
		TargetClusters:    framework.NewTargetClusters([]string{}, map[string][]int32{}),
		AvailableReplicas: availableReplicas,
		FeedKey:           utils.GetFeedKey(feed.Feed),
	}

	if sub == nil || reflect.DeepEqual(sub.Status, nil) ||
		sub.Status.BindingClusters == nil ||
		len(sub.Status.BindingClusters) == 0 {
		klog.V(5).Infof("sub %s/%s status is nil, first schedule ", sub.Namespace, sub.Name)
		return result
	}

	var annoData framework.TargetClusters
	toppologyData, isok := sub.Annotations[known.AnnoTopologyReplicas]
	if !isok {
		klog.Warningf("sub %s/%s status not nil, but no topology replicas annotations, use nil to re-schedule... ",
			sub.Namespace, sub.Name)
		return result
	}

	err := json.Unmarshal([]byte(toppologyData), &annoData)
	if err != nil || len(annoData.BindingClusters) != len(annoData.TopologyReplicas) {
		klog.Warning("subs %s/%s annotations data %s format error : %v",
			sub.Namespace, sub.Name, toppologyData, err)
		return result
	}

	for f, replicas := range annoData.Replicas {
		if f == result.FeedKey {
			result.TargetClusters.Replicas[f] = replicas
		}
	}

	for _, feedReplicas := range annoData.TopologyReplicas {
		var clusterReplicas framework.ClusterReplicas
		for f, replicas := range feedReplicas.FeedReplicas {
			if f == result.FeedKey {
				clusterReplicas = framework.ClusterReplicas{
					FeedReplicas: map[string][]framework.TopologyReplica{
						f: replicas,
					},
				}
			}
		}
		result.TargetClusters.TopologyReplicas = append(result.TargetClusters.TopologyReplicas, clusterReplicas)
	}
	result.TargetClusters.BindingClusters = append(result.TargetClusters.BindingClusters, annoData.BindingClusters...)

	klog.V(5).Infof("sub %s/%s will use topology data in annotation to schedule ... ", sub.Namespace, sub.Name)
	//result.TargetClusters = &targetClusters
	return result
}

func topologyReplicasOneFeed(sub *appsapi.Subscription, feeds *appsapi.FeedOrder, availableReplicas *framework.TargetClusters) (framework.TargetClusters, error) {
	var err error

	// load exist replicas distribution
	// all scaleUp or scaleDown operations are modifications to the TragetClusters
	topologyReplicas := NewTopologyReplicas(sub, feeds, availableReplicas)
	topologyReplicas.FeedKey = utils.GetFeedKey(feeds.Feed)

	for _, subscriber := range sub.Spec.Subscribers {
		if subscriber.Weight == 0 {
			subscriber.Weight = 1
		}
		topologyReplicas.WeightSum += subscriber.Weight
	}

	for _, subscriber := range sub.Spec.Subscribers {
		// transfer subscribers to domain map
		domain := make(map[string]string)
		for k, v := range subscriber.ClusterAffinity.MatchLabels {
			if utils.IsTopologyKey(k) {
				domain[k] = v
			}
		}

		// get this domain desired replica
		// the min replicas is 1 per topology domain
		rep := float64(*feeds.DesiredReplicas) * float64(subscriber.Weight) / float64(topologyReplicas.WeightSum)
		desiredReplica := utils.MaxInt32(1, int32(math.Round(rep)))

		// for this domain and this feed, get distributed by clusters
		totalReplica := utils.SumArrayInt32(topologyReplicas.TargetClusters.GetReplicas(topologyReplicas.FeedKey, domain))

		// determine whether to scaleup or scaledown
		switch {

		case totalReplica < desiredReplica:
			klog.V(5).Infof("will scale up subs %s/%s feed %s/%s to replica %d with current replica %d in topology domain %v",
				sub.Namespace, sub.Name, feeds.Kind, feeds.Name, desiredReplica, totalReplica, domain)
			err = topologyReplicas.scaleUp(sub, domain, desiredReplica)
			if err != nil {
				return framework.TargetClusters{}, err
			}

		case totalReplica > desiredReplica:
			klog.V(5).Infof("will scale down subs %s/%s feed %s/%s to replica %d with current replica %d in topology domain %v",
				sub.Namespace, sub.Name, feeds.Kind, feeds.Name, desiredReplica, totalReplica, domain)
			err = topologyReplicas.scaleDown(domain, desiredReplica)
			if err != nil {
				return framework.TargetClusters{}, err
			}

		default:
			klog.Infof("subs %s/%s total replicas %d same with desired replicas %d, skip scheduler...",
				sub.Namespace, sub.Name, totalReplica, desiredReplica)
		}
	}

	klog.Infof("subs %s/%s feed %s topology assign result is : %v",
		sub.Namespace, sub.Name, feeds.Name, topologyReplicas.TargetClusters)
	return *topologyReplicas.TargetClusters, nil
}

func (tr *TopologyReplicas) scaleDown(domain map[string]string, desiredReplica int32) (err error) {
	/*
		scale down scheduling policy
		0. availableRplicas(framework.TargetClusters) is sorted by cluster score
		1. if clusters more then 2 , scale down to 0 in lowest score cluster
		2. if clusters is 2, scale down target is same replicas in this 2 clusters
	*/

	var result framework.TargetClusters
	curReplicas := tr.TargetClusters.GetReplicas(tr.FeedKey, domain)
	feedReplicas := make(map[string][]int32)
	for i, v := range curReplicas {
		if v != 0 {
			result.BindingClusters = append(result.BindingClusters, tr.TargetClusters.BindingClusters[i])
			feedReplicas[tr.FeedKey] = append(feedReplicas[tr.FeedKey], v)
			result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					tr.FeedKey: {{Topology: domain, Replica: v}},
				},
			})
		}
	}
	result.Replicas = feedReplicas

	switch result.Len() {
	case 0:
		return fmt.Errorf("scale down error with nil TargetCLusters")
	case 1:
		result = framework.TargetClusters{
			BindingClusters: result.BindingClusters,
			Replicas:        map[string][]int32{tr.FeedKey: {desiredReplica}},
			TopologyReplicas: []framework.ClusterReplicas{
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						tr.FeedKey: {{Topology: domain, Replica: desiredReplica}},
					},
				},
			},
		}
		tr.TargetClusters.MergeOneFeedWithTopology(&result, false)
		return nil
	case 2:
		//tr.TargetClusters = scaleDownTwoClusters(&result, domain, feedKey, desiredReplica, result.Replicas[feedKey])
		r := scaleDownTwoClusters(&result, domain, tr.FeedKey,
			desiredReplica, result.Replicas[tr.FeedKey])
		if r != nil {
			tr.TargetClusters.MergeOneFeedWithTopology(r, false)
		} else {
			klog.Warning("scale down to two cluster with result %v domain %v feedkey %s desire replica %d is nil ",
				result, domain, tr.FeedKey, desiredReplica)
		}
		return nil
	default:
		totalReplica := utils.SumArrayInt32(result.Replicas[tr.FeedKey])
		for number := result.Len(); number > 2; number-- {
			// if current cluster more then 2, find lowest score cluster to scale down
			lowScoreCluster := findLowScoreCluster(tr.AvailableReplicas.BindingClusters, result.BindingClusters)
			if lowScoreCluster == nil {
				klog.Warningf("find low score cluster failed, find max replica cluster ")
				// TODO maybe all result binding cluster not in available replicas, scale down with average replica
				return
			}
			clusterIndex := utils.GetIndex(result.BindingClusters, *lowScoreCluster)
			if result.Replicas[tr.FeedKey][clusterIndex] > totalReplica-desiredReplica {
				// lowest cluster replicas more then the replicas to be scale down
				// scale down in this one cluster and return
				expectReplica := result.Replicas[tr.FeedKey][clusterIndex] - (totalReplica - desiredReplica)
				result.Replicas[tr.FeedKey][clusterIndex] = expectReplica
				result.TopologyReplicas[clusterIndex].FeedReplicas[tr.FeedKey][0].Replica = expectReplica

				tr.TargetClusters.MergeOneFeedWithTopology(&result, false)
				return
			} else {
				// lowest cluster replicas less then the replicas to be scale down
				// scale down to 0 and remove cluster
				// assignment desiredReplica and find next lowest cluster to scale down
				var newTopologyReplicas []framework.ClusterReplicas
				for index, d := range result.TopologyReplicas {
					if index == clusterIndex {
						continue
					}
					newTopologyReplicas = append(newTopologyReplicas, d)
				}

				result.BindingClusters = utils.RemoveStringByIndex(result.BindingClusters, clusterIndex)
				result.Replicas = map[string][]int32{tr.FeedKey: utils.RemoveInt32(result.Replicas[tr.FeedKey],
					clusterIndex)}
				result.TopologyReplicas = newTopologyReplicas

				tr.TargetClusters.MergeOneFeedWithTopology(&result, false)

				if desiredReplica == totalReplica-curReplicas[clusterIndex] {
					// return if desiredReplicas is 0
					return nil
				}
			}
		}
		r := scaleDownTwoClusters(&result, domain, tr.FeedKey,
			desiredReplica, result.Replicas[tr.FeedKey])
		if r != nil {
			tr.TargetClusters.MergeOneFeedWithTopology(r, false)
		} else {
			klog.Warning("scale down to two cluster with result %v domain %v feedkey %s desire replica %d is nil ",
				result, domain, tr.FeedKey, desiredReplica)
		}
		return nil
	}
}

func scaleDownTwoClusters(clusters *framework.TargetClusters, domain map[string]string, feedKey string, desiredReplica int32, curReplicas []int32) (result *framework.TargetClusters) {
	totalReplica := utils.SumArrayInt32(curReplicas)
	switch {
	case curReplicas[0] > desiredReplica/2 && curReplicas[1] > desiredReplica/2:
		// 2 clusters current replica more then half of desired replica
		// scale down all 2 clusters to half of desired replica
		result = &framework.TargetClusters{
			BindingClusters: clusters.BindingClusters,
			Replicas: map[string][]int32{
				feedKey: {desiredReplica / 2, desiredReplica / 2},
			},
			TopologyReplicas: []framework.ClusterReplicas{
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: desiredReplica / 2}},
					},
				},
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: desiredReplica / 2}},
					},
				},
			},
		}
	case curReplicas[0] < desiredReplica/2:
		// one cluster replicas less then half of desired replica
		// scale down another cluster all of need replica
		scaleDownReplica := curReplicas[1] - (totalReplica - desiredReplica)
		result = &framework.TargetClusters{
			BindingClusters: clusters.BindingClusters,
			Replicas: map[string][]int32{
				feedKey: {curReplicas[0], scaleDownReplica},
			},
			TopologyReplicas: []framework.ClusterReplicas{
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: curReplicas[0]}},
					},
				},
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: scaleDownReplica}},
					},
				},
			},
		}
	case curReplicas[1] < desiredReplica/2:
		scaleDownReplica := curReplicas[0] - (totalReplica - desiredReplica)
		result = &framework.TargetClusters{
			BindingClusters: clusters.BindingClusters,
			Replicas: map[string][]int32{
				feedKey: {scaleDownReplica, curReplicas[1]},
			},
			TopologyReplicas: []framework.ClusterReplicas{
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: scaleDownReplica}},
					},
				},
				{
					FeedReplicas: map[string][]framework.TopologyReplica{
						feedKey: {{Topology: domain, Replica: curReplicas[1]}},
					},
				},
			},
		}
	}
	return
}

// TargetClusters sort with descending order by score.
// Low score cluster is last one in available binding clusters.
// Besause of used for scale down , find it which in existing cluster.
func findLowScoreCluster(availableClusters, currentClusters []string) (lowestCluster *string) {
	for i := len(availableClusters) - 1; i >= 0; i-- {
		if utils.ContainsString(currentClusters, availableClusters[i]) {
			lowestCluster = &availableClusters[i]
			return
		}
	}
	return
}

func (tr *TopologyReplicas) scaleUp(sub *appsapi.Subscription, domain map[string]string, desiredReplica int32) (err error) {
	/*
		scale up scheduling policy
		0. availableRplicas(framework.TargetClusters) is sorted by cluster score
		1. the default number of cluster for scheduling replicas is 2. at most 5 clusters.
		2. more clusters will be added if they are not enough to accommodate the replicas
		3. searching for cluster, prioritize scaling up directly in existing clusters
		4. searching for new cluster, prioritize those with higher scores.
	*/

	klog.V(5).Infof("sub %s/%s will scale up with current target cluster is : %v",
		sub.Namespace, sub.Name, tr.TargetClusters)
	if tr.TargetClusters == nil || len(tr.TargetClusters.BindingClusters) == 0 {
		tr.TargetClusters = &framework.TargetClusters{
			BindingClusters: make([]string, 0),
			Replicas:        make(map[string][]int32),
		}
	}

	var result = framework.TargetClusters{}
	result.Replicas = make(map[string][]int32)

	// add current replicas which not in available binding cluster into result
	for clusterIndex, clusterId := range tr.TargetClusters.BindingClusters {
		if !utils.ContainsString(tr.AvailableReplicas.BindingClusters, clusterId) {
			keepReplica := tr.TargetClusters.TopologyReplicas[clusterIndex].GetReplica(tr.FeedKey, domain)
			result.BindingClusters = append(result.BindingClusters, clusterId)
			result.Replicas[tr.FeedKey] = append(result.Replicas[tr.FeedKey], tr.TargetClusters.Replicas[tr.FeedKey][clusterIndex])
			result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					tr.FeedKey: {
						{
							Topology: domain,
							Replica:  keepReplica,
						},
					},
				},
			})
			// subtract the replicas that need to be keep
			desiredReplica -= keepReplica
		}
	}

	klog.V(5).Infof("subs %s/%s desired replica is : %v, current replicas is : %v",
		sub.Namespace, sub.Name, desiredReplica, result)
	//var availableCluster int
	var resultFeedReplicas, curReplicas, maxReplicas []int32
	curReplicas = tr.TargetClusters.GetReplicas(tr.FeedKey, domain)

	maxReplicas = make([]int32, len(tr.AvailableReplicas.BindingClusters))
	// step 1 : find clusters, prioritize using cluster with existing replicas.
	// at least 2 clusters, and most 5 clusters.
	for i := 0; i < utils.MinInt(5, len(tr.AvailableReplicas.BindingClusters)); i++ {
		var maxReplica int32
		if len(tr.TargetClusters.BindingClusters) > i {
			maxReplica, clusterID := findExistCluster(tr.AvailableReplicas,
				tr.TargetClusters.BindingClusters[i], tr.FeedKey, domain)
			if maxReplica <= 1 {
				continue
			}
			maxReplicas[i] = maxReplica
			result.BindingClusters = append(result.BindingClusters, *clusterID)
			resultFeedReplicas = append(resultFeedReplicas, tr.TargetClusters.Replicas[tr.FeedKey][i])
			result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					tr.FeedKey: {{Topology: domain, Replica: tr.TargetClusters.TopologyReplicas[i].GetReplica(tr.FeedKey, domain)}},
				},
			})
		} else {
			var newClusterID *string
			maxReplica, newClusterID = findNewCluster(tr.AvailableReplicas,
				result.BindingClusters, tr.FeedKey, domain)
			if maxReplica <= 1 || newClusterID == nil {
				continue
			}
			maxReplicas[i] = maxReplica
			result.BindingClusters = append(result.BindingClusters, *newClusterID)
			resultFeedReplicas = append(resultFeedReplicas, 0)
			result.TopologyReplicas = append(result.TopologyReplicas, framework.ClusterReplicas{
				FeedReplicas: map[string][]framework.TopologyReplica{
					tr.FeedKey: {{Topology: domain, Replica: 0}},
				},
			})
		}
		// set replicas for result
		result.Replicas = map[string][]int32{
			tr.FeedKey: resultFeedReplicas,
		}

		if result.Len() < 2 {
			continue
		}
		// The max replicas per cluster should be greater then one-nth(n is count of clusters) of the expected replicas
		if maxReplicas[i] < desiredReplica/int32(i+1) {
			continue
		}
		// stop searching for clusters once one is found that can accommodate the required number of replicas.
		// and the latest cluster must more then 2
		if (utils.SumArrayInt32(maxReplicas) > desiredReplica-utils.SumArrayInt32(curReplicas)) &&
			result.Len() >= 2 {
			break
		}

		// stop searching for cluster once every cluster lees then 1 replicas
		if int32(result.Len()) >= desiredReplica {
			break
		}
	}

	if result.Len() < 1 {
		return fmt.Errorf("no available cluster have enough replica in domain %v", domain)
	}

	// step 3: allocate the number of replicas for the selected cluster.
	// try to set same number of replicas in each cluster

	// min replicas is 1
	averageReplica := utils.MaxInt32(int32(math.Round(float64(desiredReplica)/float64(result.Len()))), 1)

	for clusterIndex, clusterId := range result.BindingClusters {
		if !utils.ContainsString(tr.AvailableReplicas.BindingClusters, clusterId) {
			// this cluster not in availabel cluster, keep it replicas.
			continue
		}

		if maxReplicas[clusterIndex] <= averageReplica {
			// TODO: find new cluster replace this cluster
			klog.Warningf("warning of schedule subs %s/%s , cluster %s topology domain %v max available %d less then average %d, maybe pending some replicas  ",
				sub.Namespace, sub.Name, clusterId, domain, maxReplicas[clusterIndex], averageReplica)
		}

		// current replica less then average replica, scale up
		if result.TopologyReplicas[clusterIndex].GetReplica(tr.FeedKey, domain) < averageReplica {
			// if need to scale up replica more then average replica, scale up
			if desiredReplica > averageReplica-utils.SumArrayInt32(result.GetReplicas(tr.FeedKey, domain)) {
				// scale up to average replica, but not enough , find next cluster
				result.TopologyReplicas[clusterIndex].SetReplica(tr.FeedKey, domain, averageReplica)
				desiredReplica -= averageReplica
			} else {
				// scale up add desired replica and break
				result.TopologyReplicas[clusterIndex].AddReplica(tr.FeedKey, domain, desiredReplica)
				result.Replicas[tr.FeedKey][clusterIndex] += desiredReplica
				break
			}
		} else {
			desiredReplica -= result.TopologyReplicas[clusterIndex].GetReplica(tr.FeedKey, domain)
		}
		result.Replicas[tr.FeedKey][clusterIndex] = result.TopologyReplicas[clusterIndex].GetReplica(tr.FeedKey, domain)
	}

	klog.V(5).Infof("sub %s/%s scale up result : %v",
		sub.Namespace, sub.Name, result)
	tr.TargetClusters.MergeOneFeedWithTopology(&result, false)
	return nil
}

// TargetClusters sort with descending order by score.
func findExistCluster(availableReplicas *framework.TargetClusters, cluster, feedKey string, domain map[string]string) (int32, *string) {
	if len(availableReplicas.TopologyReplicas) != len(availableReplicas.BindingClusters) {
		return 0, nil
	}
	for index, id := range availableReplicas.BindingClusters {
		if cluster == id {
			for _, replica := range availableReplicas.TopologyReplicas[index].FeedReplicas[feedKey] {
				if reflect.DeepEqual(replica.Topology, domain) {
					return replica.Replica, &id
				}
			}
		}
	}
	return 0, nil
}

// TargetClusters sort with descending order by score.
// Besause of used for scale up , start from the first one and not in existing clusters.
func findNewCluster(availableReplicas *framework.TargetClusters,
	clusters []string, feedKey string, domain map[string]string) (int32, *string) {
	// available replicas is ascending by score , we need find bigest score cluster when scale up
	if len(availableReplicas.BindingClusters) < 1 || len(availableReplicas.TopologyReplicas) < 1 {
		return 0, nil
	}
	//for index := len(availableReplicas.BindingClusters) - 1; index >= 0; index-- {
	//clusterID := availableReplicas.BindingClusters[index]
	for index, clusterID := range availableReplicas.BindingClusters {
		if !utils.ContainsString(clusters, clusterID) {
			for _, v := range availableReplicas.TopologyReplicas[index].FeedReplicas[feedKey] {
				if reflect.DeepEqual(v.Topology, domain) {
					return v.Replica, &clusterID
				}
			}
		}
	}
	return 0, nil
}
