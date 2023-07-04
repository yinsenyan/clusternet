package topologyassigner

import (
	"reflect"
	"testing"

	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/utils"
)

var (
	defaultTopology = map[string]string{
		"region.topology.camp.io/name":         "ap-guangzhou",
		"zone.topology.camp.io/ap-guangzhou-1": "true",
	}

	gz2Topology = map[string]string{
		"region.topology.camp.io/name":         "ap-guangzhou",
		"zone.topology.camp.io/ap-guangzhou-2": "true",
	}

	defaultAvailableReplicas = &framework.TargetClusters{
		BindingClusters: []string{"cls1", "cls2", "cls3", "cls4", "cls5"},
		Replicas: map[string][]int32{
			utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {90, 200, 320, 500, 600},
			utils.GetFeedKey(newDeploymentFeed("f3")):      {100, 220, 330, 550, 660},
		},
		Score: []int64{480, 400, 300, 200, 100},
		TopologyReplicas: []framework.ClusterReplicas{
			{
				FeedReplicas: map[string][]framework.TopologyReplica{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 40}, {Topology: gz2Topology, Replica: 50}},
					utils.GetFeedKey(newDeploymentFeed("f3")):      {{Topology: defaultTopology, Replica: 50}, {Topology: gz2Topology, Replica: 50}},
				},
			},
			{
				FeedReplicas: map[string][]framework.TopologyReplica{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 200}},
					utils.GetFeedKey(newDeploymentFeed("f3")):      {{Topology: defaultTopology, Replica: 220}},
				},
			},
			{
				FeedReplicas: map[string][]framework.TopologyReplica{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 120}, {Topology: gz2Topology, Replica: 180}},
					utils.GetFeedKey(newDeploymentFeed("f3")):      {{Topology: defaultTopology, Replica: 130}, {Topology: gz2Topology, Replica: 200}},
				},
			},
			{
				FeedReplicas: map[string][]framework.TopologyReplica{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 100}, {Topology: gz2Topology, Replica: 400}},
					utils.GetFeedKey(newDeploymentFeed("f3")):      {{Topology: defaultTopology, Replica: 130}, {Topology: gz2Topology, Replica: 420}},
				},
			},
			{
				FeedReplicas: map[string][]framework.TopologyReplica{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 180}},
					utils.GetFeedKey(newDeploymentFeed("f3")):      {{Topology: defaultTopology, Replica: 200}},
				},
			},
		},
	}
)

func TestScaleDown(t *testing.T) {
	tr := NewTopologyReplicas(nil, nil, defaultAvailableReplicas)
	tr.FeedKey = utils.GetFeedKey(newStatefulsetPlusFeed("f1"))
	tr.AvailableReplicas = defaultAvailableReplicas
	type args struct {
		clusters       *framework.TargetClusters
		domain         map[string]string
		desiredReplica int32
	}
	tests := []struct {
		name  string
		input args
		want  *framework.TargetClusters
	}{
		{
			name: "scale down 1 clusters ",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 6,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {6},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 6}},
						},
					},
				},
			},
		},
		{
			name: "scale down 2 clusters to same replicas",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1", "cls2"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 10},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 16,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {8, 8},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 8}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 8}},
						},
					},
				},
			},
		},
		{
			name: "scale down 2 clusters with one cluster has more replica",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1", "cls2"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {4, 16},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 4}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 16}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 10,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {4, 6},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 4}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 6}},
						},
					},
				},
			},
		},
		{
			name: "scale down 3 clusters with lowest clusters ",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1", "cls2", "cls3"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 10, 4},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 4}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 22,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2", "cls3"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 10, 2},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2}},
						},
					},
				},
			},
		},
		{
			name: "scale down 3 clusters to 2 clusters directly",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1", "cls2", "cls3"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 10, 8},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 8}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 18,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {9, 9},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 9}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 9}},
						},
					},
				},
			},
		},
		{
			name: "scale down 3 clusters to 2 clusters",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1", "cls2", "cls3"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {8, 10, 4},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 8}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 4}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 12,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {6, 6},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 6}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 6}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr.TargetClusters = tt.input.clusters
			err := tr.scaleDown(tt.input.domain, tt.input.desiredReplica)
			if err != nil {
				t.Errorf("\n test %s error: %v", tt.name, err)
			}
			if !reflect.DeepEqual(tr.TargetClusters, tt.want) {
				t.Errorf("\ntest %s error : \nwant %v\n got %v", tt.name, tt.want, tr.TargetClusters)
			}
		})
	}
}

func TestScaleUp(t *testing.T) {
	tr := NewTopologyReplicas(nil, nil, defaultAvailableReplicas)
	tr.FeedKey = utils.GetFeedKey(newStatefulsetPlusFeed("f1"))
	tr.AvailableReplicas = defaultAvailableReplicas

	type args struct {
		clusters       *framework.TargetClusters
		domain         map[string]string
		desiredReplica int32
	}
	tests := []struct {
		name  string
		input args
		want  *framework.TargetClusters
	}{
		{
			name: "scale up with first deploy",
			input: args{
				clusters:       nil,
				domain:         defaultTopology,
				desiredReplica: 20,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 10},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
						},
					},
				},
			},
		},
		{
			name: "scale up with 1 cluster to 2 clusters",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 12,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 2},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2}},
						},
					},
				},
			},
		},
		{
			name: "scale up with 5 clusters",
			input: args{
				clusters: &framework.TargetClusters{
					BindingClusters: []string{"cls1", "cls2", "cls3", "cls4", "cls5"},
					Replicas: map[string][]int32{
						utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {10, 10, 10, 10, 10},
					},
					TopologyReplicas: []framework.ClusterReplicas{
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
						{
							FeedReplicas: map[string][]framework.TopologyReplica{
								utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 10}},
							},
						},
					},
				},
				domain:         defaultTopology,
				desiredReplica: 10000,
			},
			want: &framework.TargetClusters{
				BindingClusters: []string{"cls1", "cls2", "cls3", "cls4", "cls5"},
				Replicas: map[string][]int32{
					utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {2000, 2000, 2000, 2000, 2000},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2000}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2000}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2000}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2000}},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							utils.GetFeedKey(newStatefulsetPlusFeed("f1")): {{Topology: defaultTopology, Replica: 2000}},
						},
					},
				},
			},
		},
		// TODO add a case that current cluster not in available cluster, and use new cluster
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tr.TargetClusters = tt.input.clusters.DeepCopy()
			err := tr.scaleUp(subTemplate, tt.input.domain, tt.input.desiredReplica)
			if err != nil {
				t.Errorf("\ntest %s error: %v", tt.name, err)
			}
			if !reflect.DeepEqual(tr.TargetClusters, tt.want) {
				t.Errorf("\ntest %s error : \nwant %v\n got %v", tt.name, tt.want, tr.TargetClusters)
			}
		})
	}
}

func Test_FindLowCluster(t *testing.T) {
	tests := []struct {
		name      string
		available []string
		current   []string
		want      string
	}{
		{
			name:      "get last one",
			available: []string{"cls-pz4wglp7/cls-pz4wglp7", "cls-i9et79ll/cls-i9et79ll", "cls-3daxhf1b/cls-3daxhf1b"},
			current:   []string{"cls-3daxhf1b/cls-3daxhf1b", "cls-i9et79ll/cls-i9et79ll", "cls-pz4wglp7/cls-pz4wglp7"},
			want:      "cls-3daxhf1b/cls-3daxhf1b",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := findLowScoreCluster(tt.available, tt.current)
			if *got != tt.want {
				t.Errorf("\ntest %s got %v, want %v", tt.name, *got, tt.want)
			}
		})
	}
}
