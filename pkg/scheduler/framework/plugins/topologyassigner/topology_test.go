package topologyassigner

import (
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/utils"
)

var (
	subTemplate = &appsapi.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{
				known.AnnoEnableTopology: "true",
				//AnnoTopologyReplicas: "{}",
			},
		},
		Spec: appsapi.SubscriptionSpec{
			SchedulingStrategy: appsapi.DividingSchedulingStrategyType,
			DividingScheduling: &appsapi.DividingScheduling{
				Type: appsapi.StaticReplicaDividingType,
			},
			Feeds: []appsapi.Feed{
				newStatefulsetPlusFeed("f1"),
				newServiceFeed("f2"),
				newDeploymentFeed("f3"),
			},
			Subscribers: []appsapi.Subscriber{
				{
					ClusterAffinity: &metav1.LabelSelector{
						MatchLabels: defaultTopology,
					},
					Weight: 10,
				},
				{
					ClusterAffinity: &metav1.LabelSelector{
						MatchLabels: gz2Topology,
					},
					Weight: 10,
				},
			},
		},
	}

	feedTemplate = &appsapi.FeedInventory{
		Spec: appsapi.FeedInventorySpec{
			Feeds: []appsapi.FeedOrder{
				{
					Feed:            newStatefulsetPlusFeed("f1"),
					DesiredReplicas: utils.Int32Ptr(20),
				},
				{
					Feed: newServiceFeed("f2"),
				},
				{
					Feed:            newDeploymentFeed("f3"),
					DesiredReplicas: utils.Int32Ptr(20),
				},
			},
		},
	}
)

// stspFeedKey = "platform.stke/v1alpha1/StatefulSetPlus/default/localstatus"
func newStatefulsetPlusFeed(name string) appsapi.Feed {
	return appsapi.Feed{
		Kind:       "StatefulSetPlus",
		APIVersion: "platform.stke/v1alpha1",
		Namespace:  "default",
		Name:       name,
	}
}

func newDeploymentFeed(name string) appsapi.Feed {
	return appsapi.Feed{
		Kind:       "Deployment",
		APIVersion: "apps/v1",
		Namespace:  "default",
		Name:       name,
	}
}

func newServiceFeed(name string) appsapi.Feed {
	return appsapi.Feed{
		Kind:       "Service",
		APIVersion: "v1",
		Namespace:  "default",
		Name:       name,
	}
}

func TestTopologyReplicasOneFeed(t *testing.T) {
	type args struct {
		sub              *appsapi.Subscription
		feeds            *appsapi.FeedInventory
		availableRplicas *framework.TargetClusters
	}

	tests := []struct {
		name  string
		input args
		want  *framework.TargetClusters
	}{
		{
			name: "topology divide replicass",
			input: args{
				sub:              subTemplate,
				feeds:            feedTemplate,
				availableRplicas: defaultAvailableReplicas,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls5", "cls4", "cls3"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {5, 10, 5},
					utils.GetFeedKey(newServiceFeed("f2")):         {},
					utils.GetFeedKey(newDeploymentFeed("f3")):      {5, 10, 5},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {
								framework.TopologyReplica{Replica: 5, Topology: defaultTopology},
							},
							utils.GetFeedKey(newDeploymentFeed("f3")): {
								framework.TopologyReplica{Replica: 5, Topology: defaultTopology},
							},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {
								framework.TopologyReplica{Replica: 5, Topology: defaultTopology},
								framework.TopologyReplica{Replica: 5, Topology: gz2Topology},
							},
							utils.GetFeedKey(newDeploymentFeed("f3")): {
								framework.TopologyReplica{Replica: 5, Topology: defaultTopology},
								framework.TopologyReplica{Replica: 5, Topology: gz2Topology},
							},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {
								framework.TopologyReplica{Replica: 5, Topology: gz2Topology},
							},
							utils.GetFeedKey(newDeploymentFeed("f3")): {
								framework.TopologyReplica{Replica: 5, Topology: gz2Topology},
							},
						},
					},
				},
			},
		},
		{
			name: "one feed scale down",
			input: args{
				sub: &appsapi.Subscription{
					ObjectMeta: metav1.ObjectMeta{
						Annotations: map[string]string{
							known.AnnoEnableTopology: "true",
							//AnnoTopologyReplicas: "{}",
						},
					},
					Spec: appsapi.SubscriptionSpec{
						SchedulingStrategy: appsapi.DividingSchedulingStrategyType,
						DividingScheduling: &appsapi.DividingScheduling{
							Type: appsapi.StaticReplicaDividingType,
						},
						Feeds: []appsapi.Feed{
							newStatefulsetPlusFeed("f1"),
						},
						Subscribers: []appsapi.Subscriber{
							{
								ClusterAffinity: &metav1.LabelSelector{
									MatchLabels: defaultTopology,
								},
								Weight: 10,
							},
							{
								ClusterAffinity: &metav1.LabelSelector{
									MatchLabels: gz2Topology,
								},
								Weight: 10,
							},
						},
					},
					Status: appsapi.SubscriptionStatus{
						BindingClusters: []string{"cls1", "cls2"},
						Replicas:        map[string][]int32{utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {5, 5}},
					},
				},
				feeds: &appsapi.FeedInventory{
					Spec: appsapi.FeedInventorySpec{
						Feeds: []appsapi.FeedOrder{{DesiredReplicas: utils.Int32Ptr(4), Feed: newStatefulsetPlusFeed("f1")}},
					},
				},
				availableRplicas: defaultAvailableReplicas,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2", "cls4"},
				Replicas:        map[string][]int32{utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {2, 1, 1}},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {
								{Replica: 1, Topology: defaultTopology}, {Replica: 1, Topology: gz2Topology},
							},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {
								{Replica: 1, Topology: defaultTopology},
							},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {
								{Replica: 1, Topology: gz2Topology},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := StaticTopologyDivideReplicas(tt.input.sub, tt.input.feeds, tt.input.availableRplicas)
			if err != nil {
				t.Errorf("\n test %s error: %v", tt.name, err)
			}
			if !reflect.DeepEqual(result, *tt.want) {
				t.Errorf("\ntest %s error : \nwant %v\n got %v", tt.name, tt.want, result)
			}
		})
	}
}

func Test_One(t *testing.T) {
	sub := &appsapi.Subscription{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test",
			Annotations: map[string]string{
				known.AnnoTopologyReplicas: `{
					"bindingClusters":["cls-cw53vmvq/cls-cw53vmvq","cls-h2hx3tqu/cls-h2hx3tqu","cls-q73xuilw/cls-q73xuilw"],
					"replicas":{"autoscaling/v2beta2/HorizontalPodAutoscaler/prj-dbqhtbpf-development/kingslitest-mmm":[],
					  "platform.stke/v1alpha1/StatefulSetPlus/prj-dbqhtbpf-development/kingslitest-mmm":[2,3,1]},
					"topologyReplicas":[
					  {"feedReplicas":{"platform.stke/v1alpha1/StatefulSetPlus/prj-dbqhtbpf-development/kingslitest-mmm":[
						{"topology":{"region.topology.camp.io/name":"ap-guangzhou","zone.topology.camp.io/ap-guangzhou-4":"true"},"replica":1},
						{"topology":{"region.topology.camp.io/name":"ap-guangzhou","zone.topology.camp.io/ap-guangzhou-3":"true"},"replica":1}
					  ]}},
					  {"feedReplicas":{"platform.stke/v1alpha1/StatefulSetPlus/prj-dbqhtbpf-development/kingslitest-mmm":[
						{"topology":{"region.topology.camp.io/name":"ap-guangzhou","zone.topology.camp.io/ap-guangzhou-6":"true"},"replica":1},
						{"topology":{"region.topology.camp.io/name":"ap-guangzhou","zone.topology.camp.io/ap-guangzhou-4":"true"},"replica":1},
						{"topology":{"region.topology.camp.io/name":"ap-guangzhou","zone.topology.camp.io/ap-guangzhou-3":"true"},"replica":1}
					  ]}},
					  {"feedReplicas":{"platform.stke/v1alpha1/StatefulSetPlus/prj-dbqhtbpf-development/kingslitest-mmm":[
						{"topology":{"region.topology.camp.io/name":"ap-guangzhou","zone.topology.camp.io/ap-guangzhou-6":"true"},"replica":1}
					  ]}}
					],"score":null}`,
			},
		},
		Spec: appsapi.SubscriptionSpec{
			DividingScheduling: &appsapi.DividingScheduling{
				Type: appsapi.StaticReplicaDividingType,
			},
			SchedulingStrategy: appsapi.DividingSchedulingStrategyType,
			Feeds: []appsapi.Feed{
				{
					Namespace:  "prj-dbqhtbpf-development",
					Name:       "kingslitest-mmm",
					Kind:       "StatefulSetPlus",
					APIVersion: "platform.stke/v1alpha1",
				},
				{
					APIVersion: "autoscaling/v2beta2",
					Kind:       "HorizontalPodAutoscaler",
					Name:       "kingslitest-mmm",
					Namespace:  "prj-dbqhtbpf-development",
				},
				{
					APIVersion: "v1",
					Kind:       "ConfigMap",
					Name:       "kingslitest-cm-0.0.1",
					Namespace:  "prj-dbqhtbpf-development",
				},
			},
			Subscribers: []appsapi.Subscriber{
				{
					ClusterAffinity: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"region.topology.camp.io/name":         "ap-guangzhou",
							"zone.topology.camp.io/ap-guangzhou-3": "true",
						},
					},
					Weight: 33,
				},
				{
					ClusterAffinity: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"region.topology.camp.io/name":         "ap-guangzhou",
							"zone.topology.camp.io/ap-guangzhou-4": "true",
						},
					},
					Weight: 33,
				},
				{
					ClusterAffinity: &metav1.LabelSelector{
						MatchLabels: map[string]string{
							"region.topology.camp.io/name":         "ap-guangzhou",
							"zone.topology.camp.io/ap-guangzhou-6": "true",
						},
					},
					Weight: 33,
				},
			},
		},
		Status: appsapi.SubscriptionStatus{
			BindingClusters: []string{"cls-cw53vmvq/cls-cw53vmvq", "cls-h2hx3tqu/cls-h2hx3tqu", "cls-q73xuilw/cls-q73xuilw"},
		},
	}
	feeds := &appsapi.FeedInventory{
		Spec: appsapi.FeedInventorySpec{
			Feeds: []appsapi.FeedOrder{
				{
					Feed: appsapi.Feed{
						Namespace:  "prj-dbqhtbpf-development",
						Name:       "kingslitest-mmm",
						Kind:       "StatefulSetPlus",
						APIVersion: "platform.stke/v1alpha1",
					},
					DesiredReplicas:     utils.Int32Ptr(6),
					ReplicaRequirements: appsapi.ReplicaRequirements{},
				},
				{
					Feed: appsapi.Feed{
						APIVersion: "autoscaling/v2beta2",
						Kind:       "HorizontalPodAutoscaler",
						Name:       "kingslitest-mmm",
						Namespace:  "prj-dbqhtbpf-development",
					},
				},
				{
					Feed: appsapi.Feed{
						APIVersion: "v1",
						Kind:       "ConfigMap",
						Name:       "kingslitest-cm-0.0.1",
						Namespace:  "prj-dbqhtbpf-development",
					},
				},
			},
		},
	}
	result, err := StaticTopologyDivideReplicas(sub, feeds, nil)
	if err != nil {
		t.Errorf("\n test error: %v", err)
	}
	if !reflect.DeepEqual(result, framework.TargetClusters{BindingClusters: []string{"ccc"}}) {
		t.Errorf("\ntest one error : \ngot %v", result)
	}
}
