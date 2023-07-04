package interfaces

import (
	"reflect"
	"testing"
)

func TestTargetClustersWithFirstScheduling_Merge(t *testing.T) {
	source := &TargetClusters{
		BindingClusters: []string{},
		Replicas:        map[string][]int32{},
	}

	tests := []struct {
		name   string
		b      *TargetClusters
		result *TargetClusters
	}{
		{
			name: "f1 and f2 in c1 and c2",
			b: &TargetClusters{
				BindingClusters: []string{"c1", "c2"},
				Replicas: map[string][]int32{
					"f1": {1, 1},
					"f2": {1, 2},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2"},
				Replicas: map[string][]int32{
					"f1": {1, 1},
					"f2": {1, 2},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tcp := source.DeepCopy()
			for k, v := range tt.b.Replicas {
				tcp.MergeOneFeed(&TargetClusters{
					BindingClusters: tt.b.BindingClusters,
					Replicas:        map[string][]int32{k: v},
				})
			}
			if !reflect.DeepEqual(tcp, tt.result) {
				t.Errorf("Merge() %s gotResponse = %v, want %v", tt.name, tcp, tt.result)
			}
		})
	}
}

func TestTargetClusters_Merge(t *testing.T) {
	template := &TargetClusters{
		BindingClusters: []string{"c1", "c2", "c3"},
		Replicas: map[string][]int32{
			"f1": {1, 2, 3},
			"f2": {},
			"f3": {1, 0, 1},
		},
	}
	tests := []struct {
		name   string
		b      *TargetClusters
		result *TargetClusters
	}{
		{
			name: "f1 and f3 in c2 and c3",
			b: &TargetClusters{
				BindingClusters: []string{"c2", "c3"},
				Replicas: map[string][]int32{
					"f1": {1, 2},
					"f3": {1, 0},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3"},
				Replicas: map[string][]int32{
					"f1": {1, 3, 5},
					"f2": {},
					"f3": {1, 1, 1},
				},
			},
		},
		{
			name: "f1 and f3 in c2 and c4",
			b: &TargetClusters{
				BindingClusters: []string{"c2", "c4"},
				Replicas: map[string][]int32{
					"f1": {1, 2},
					"f3": {1, 2},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3", "c4"},
				Replicas: map[string][]int32{
					"f1": {1, 3, 3, 2},
					"f2": {},
					"f3": {1, 1, 1, 2},
				},
			},
		},
		{
			name: "f3, f4 and f5 in c2 and c4",
			b: &TargetClusters{
				BindingClusters: []string{"c2", "c4"},
				Replicas: map[string][]int32{
					"f3": {1, 2},
					"f4": {1, 2},
					"f5": {2, 3},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3", "c4"},
				Replicas: map[string][]int32{
					"f1": {1, 2, 3, 0},
					"f2": {},
					"f3": {1, 1, 1, 2},
					"f4": {0, 1, 0, 2},
					"f5": {0, 2, 0, 3},
				},
			},
		},
		{
			name: "f3, f4 and f5 in c0 and c4",
			b: &TargetClusters{
				BindingClusters: []string{"c0", "c4"},
				Replicas: map[string][]int32{
					"f3": {1, 2},
					"f4": {1, 2},
					"f5": {},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3", "c0", "c4"},
				Replicas: map[string][]int32{
					"f1": {1, 2, 3, 0, 0},
					"f2": {},
					"f3": {1, 0, 1, 1, 2},
					"f4": {0, 0, 0, 1, 2},
					"f5": {},
				},
			},
		},
		{
			name: "unsorted cluster",
			b: &TargetClusters{
				BindingClusters: []string{"c1", "c3", "c2"},
				Replicas: map[string][]int32{
					"f2": {1, 2, 0},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3"},
				Replicas: map[string][]int32{
					"f1": {1, 2, 3},
					"f2": {1, 0, 2},
					"f3": {1, 0, 1},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tcp := template.DeepCopy()
			for k, v := range tt.b.Replicas {
				tcp.MergeOneFeed(&TargetClusters{
					BindingClusters: tt.b.BindingClusters,
					Replicas:        map[string][]int32{k: v},
				})
			}
			if !reflect.DeepEqual(tcp, tt.result) {
				t.Errorf("Merge() %s gotResponse = %v, want %v", tt.name, tcp, tt.result)
			}
		})
	}
}

func TestTargetCLusters_MergeOneFeed(t *testing.T) {
	template := &TargetClusters{
		BindingClusters: []string{"c1", "c2", "c3"},
		Replicas: map[string][]int32{
			"f1": {1, 2, 3},
			"f2": {},
			"f3": {1, 0, 3},
		},
	}
	tests := []struct {
		name   string
		b      *TargetClusters
		result *TargetClusters
	}{
		{
			name: "merge one feed",
			b: &TargetClusters{
				BindingClusters: []string{
					"c1", "c2", "c3",
				},
				Replicas: map[string][]int32{
					"f1": {4, 4, 5},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3"},
				Replicas: map[string][]int32{
					"f1": {5, 6, 8},
					"f2": {},
					"f3": {1, 0, 3},
				},
			},
		},
		{
			name: "merge not exist feed",
			b: &TargetClusters{
				BindingClusters: []string{
					"c1", "c2", "c3",
				},
				Replicas: map[string][]int32{
					"f4": {1, 1, 1},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{
					"c1", "c2", "c3",
				},
				Replicas: map[string][]int32{
					"f1": {1, 2, 3},
					"f2": {},
					"f3": {1, 0, 3},
					"f4": {1, 1, 1},
				},
			},
		},
		{
			name: "merge not exist cluster",
			b: &TargetClusters{
				BindingClusters: []string{
					"c1", "c4",
				},
				Replicas: map[string][]int32{
					"f1": {1, 1},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{
					"c1", "c2", "c3", "c4",
				},
				Replicas: map[string][]int32{
					"f1": {2, 2, 3, 1},
					"f2": {},
					"f3": {1, 0, 3, 0},
				},
			},
		},
		{
			name: "merge not exist cluster and feed",
			b: &TargetClusters{
				BindingClusters: []string{
					"c1", "c4",
				},
				Replicas: map[string][]int32{
					"f2": {1, 4},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{
					"c1", "c2", "c3", "c4",
				},
				Replicas: map[string][]int32{
					"f1": {1, 2, 3, 0},
					"f2": {1, 0, 0, 4},
					"f3": {1, 0, 3, 0},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tcopy := template.DeepCopy()
			tcopy.MergeOneFeed(tt.b)
			if !reflect.DeepEqual(tcopy, tt.result) {
				t.Errorf("MergeOneFeed() %s \ngot = %v\nwant= %v", tt.name, tcopy, tt.result)
			}
		})
	}
}

var template = &TargetClusters{
	BindingClusters: []string{"c1", "c2", "c3"},
	Replicas: map[string][]int32{
		"f1": {1, 2, 3},
		"f2": {},
		"f3": {1, 0, 3},
	},
	TopologyReplicas: []ClusterReplicas{
		{FeedReplicas: map[string][]TopologyReplica{
			"f1": {{Topology: map[string]string{"foo": "bar"}, Replica: 1}},
			"f3": {{Topology: map[string]string{"foo": "bar"}, Replica: 1}},
		}},
		{FeedReplicas: map[string][]TopologyReplica{
			"f1": {{Topology: map[string]string{"foo": "bar"}, Replica: 2}},
		}},
		{FeedReplicas: map[string][]TopologyReplica{
			"f1": {{Topology: map[string]string{"foo": "bar"}, Replica: 3}},
			"f3": {{Topology: map[string]string{"foo": "bar"}, Replica: 3}},
		}},
	},
}

func TestTargetClusters_MergeOneFeedWithTopology(t *testing.T) {
	tests := []struct {
		name   string
		b      *TargetClusters
		result *TargetClusters
	}{
		{
			name: "merge feed f3 with 2 clusters",
			b: &TargetClusters{
				BindingClusters: []string{"c2", "c3"},
				Replicas: map[string][]int32{
					"f3": {2, 3},
				},
				TopologyReplicas: []ClusterReplicas{
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f3": {{Topology: map[string]string{"foo": "bar"}, Replica: 2}},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f3": {{Topology: map[string]string{"foo": "bar"}, Replica: 3}},
						},
					},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3"},
				Replicas: map[string][]int32{
					"f1": {1, 2, 3},
					"f2": {},
					"f3": {0, 2, 3},
				},
				TopologyReplicas: []ClusterReplicas{
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Replica: 1, Topology: map[string]string{"foo": "bar"}}},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Replica: 2, Topology: map[string]string{"foo": "bar"}}},
							"f3": {{Replica: 2, Topology: map[string]string{"foo": "bar"}}},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Replica: 3, Topology: map[string]string{"foo": "bar"}}},
							"f3": {{Replica: 3, Topology: map[string]string{"foo": "bar"}}},
						},
					},
				},
			},
		},
		{
			name: "merge feed f1 with 2 clusters",
			b: &TargetClusters{
				BindingClusters: []string{"c1", "c2"},
				Replicas: map[string][]int32{
					"f1": {3, 2},
				},
				TopologyReplicas: []ClusterReplicas{
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Topology: map[string]string{"foo": "bar"}, Replica: 3}},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Topology: map[string]string{"foo": "bar"}, Replica: 2}},
						},
					},
				},
			},
			result: &TargetClusters{
				BindingClusters: []string{"c1", "c2", "c3"},
				Replicas: map[string][]int32{
					"f1": {3, 2, 0},
					"f2": {},
					"f3": {1, 0, 3},
				},
				TopologyReplicas: []ClusterReplicas{
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Replica: 3, Topology: map[string]string{"foo": "bar"}}},
							"f3": {{Replica: 1, Topology: map[string]string{"foo": "bar"}}},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f1": {{Replica: 2, Topology: map[string]string{"foo": "bar"}}},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"f3": {{Replica: 3, Topology: map[string]string{"foo": "bar"}}},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t1 *testing.T) {
			tcopy := template.DeepCopy()
			tcopy.MergeOneFeedWithTopology(tt.b, false)
			if !reflect.DeepEqual(tcopy, tt.result) {
				t.Errorf("MergeOneFeedWithTopology() %s \ngot = %v\nwant= %v", tt.name, tcopy, tt.result)
			}
		})
	}
}

func Test_One(t *testing.T) {
	a := &TargetClusters{
		BindingClusters: []string{"c1", "c2", "c3", "c4"},
		Replicas:        map[string][]int32{"f1": {5, 6, 4, 5}},
		TopologyReplicas: []ClusterReplicas{
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {{Topology: map[string]string{"zone": "gz1"}, Replica: 5}},
				},
			},
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {{Topology: map[string]string{"zone": "gz1"}, Replica: 6}},
				},
			},
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {{Topology: map[string]string{"zone": "gz1"}, Replica: 4}},
				},
			},
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {{Topology: map[string]string{"zone": "gz1"}, Replica: 5}},
				},
			},
		},
	}

	b := &TargetClusters{
		BindingClusters: []string{"c1", "c2", "c3", "c4"},
		Replicas:        map[string][]int32{"f1": {-5, -5, -4, -4}},
		TopologyReplicas: []ClusterReplicas{
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {
						{Topology: map[string]string{"zone": "gz1"}, Replica: -5},
					},
				},
			},
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {
						{Topology: map[string]string{"zone": "gz1"}, Replica: -5},
					},
				},
			},
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {
						{Topology: map[string]string{"zone": "gz1"}, Replica: -4},
					},
				},
			},
			{
				FeedReplicas: map[string][]TopologyReplica{
					"f1": {
						{Topology: map[string]string{"zone": "gz1"}, Replica: -4},
					},
				},
			},
		},
	}
	a.MergeOneFeedWithTopology(b, true)
	if !reflect.DeepEqual(a, TargetClusters{}) {
		t.Errorf("test error : got %v", a)
	}
}
