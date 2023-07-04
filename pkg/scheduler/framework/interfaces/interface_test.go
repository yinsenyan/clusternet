package interfaces

import (
	"reflect"
	"testing"
)

func Test_ToTargetClusters(t *testing.T) {
	tests := []struct {
		name     string
		input    ClusterScoreList
		feedKeys []string
		want     TargetClusters
	}{
		{
			name:     "one cluster and two topologys",
			feedKeys: []string{"foo"},
			input: []ClusterScore{
				{
					NamespacedName: "cls1/cls1",
					Score:          100,
					MaxAvailableReplicas: []map[string]int32{
						{
							"region=gz,zone=gz-1": 10,
							"region=gz,zone=gz-2": 20,
						},
					},
				},
			},
			want: TargetClusters{
				BindingClusters: []string{"cls1/cls1"},
				Replicas:        map[string][]int32{"foo": {30}},
				TopologyReplicas: []ClusterReplicas{
					{
						FeedReplicas: map[string][]TopologyReplica{
							"foo": {
								TopologyReplica{
									Topology: map[string]string{
										"region": "gz",
										"zone":   "gz-1",
									},
									Replica: 10,
								},
								TopologyReplica{
									Topology: map[string]string{
										"region": "gz",
										"zone":   "gz-2",
									},
									Replica: 20,
								},
							},
						},
					},
				},
				Score: []int64{100},
			},
		},
		{
			name:     "two cluster and one feed and two topology",
			feedKeys: []string{"foo"},
			input: []ClusterScore{
				{
					NamespacedName: "cls1",
					Score:          100,
					MaxAvailableReplicas: []map[string]int32{
						{
							"region=gz,zone=gz-1": 10,
							"region=gz,zone=gz-2": 20,
						},
					},
				},
				{
					NamespacedName: "cls2",
					Score:          200,
					MaxAvailableReplicas: []map[string]int32{
						{
							"region=gz,zone=gz-1": 50,
							"region=gz,zone=gz-2": 90,
						},
					},
				},
			},
			want: TargetClusters{
				BindingClusters: []string{"cls1", "cls2"},
				Replicas: map[string][]int32{
					"foo": {60, 110},
				},
				Score: []int64{100, 200},
				TopologyReplicas: []ClusterReplicas{
					{
						FeedReplicas: map[string][]TopologyReplica{
							"foo": {
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-1"},
									Replica:  10,
								},
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-2"},
									Replica:  20,
								},
							},
						},
					},
					{
						FeedReplicas: map[string][]TopologyReplica{
							"foo": {
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-1"},
									Replica:  50,
								},
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-2"},
									Replica:  90,
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			output := tt.input.ToTargetClusters(tt.feedKeys)
			if !reflect.DeepEqual(output, tt.want) {
				t.Errorf("test %s error , \n got %v, \nwant %v", tt.name, output, tt.want)
			}
		})
	}
}
