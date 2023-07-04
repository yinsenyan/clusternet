package topologypostbinder

import (
	"reflect"
	"testing"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
)

func TestGeneratorLocal(t *testing.T) {
	tests := []struct {
		name  string
		input framework.TargetClusters
		want  []appsapi.Localization
	}{
		{
			name: "one",
			input: framework.TargetClusters{
				BindingClusters: []string{"c1/c1", "c2/c2"},
				Replicas: map[string][]int32{
					"w1": {10, 20},
					"w2": {},
				},
				TopologyReplicas: []framework.ClusterReplicas{
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							"w1": {
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-1"},
									Replica:  5,
								},
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-2"},
									Replica:  5,
								},
							},
						},
					},
					{
						FeedReplicas: map[string][]framework.TopologyReplica{
							"w1": {
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-1"},
									Replica:  10,
								},
								{
									Topology: map[string]string{"region": "gz", "zone": "gz-2"},
									Replica:  10,
								},
							},
						},
					},
				},
			},
			want: []appsapi.Localization{},
		},
	}

	for _, tt := range tests {
		got := generatorLocal("sub", tt.input)
		if !reflect.DeepEqual(got, tt.want) {
			t.Errorf("test %s error , \n got %v, \nwant %v", tt.name, got, tt.want)
			t.Error(got[0].Spec.Overrides[0].Value)
		}
	}

}
