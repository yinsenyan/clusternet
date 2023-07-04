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

package predictor

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	clusterapi "github.com/clusternet/clusternet/pkg/apis/clusters/v1beta1"
	proxiesapi "github.com/clusternet/clusternet/pkg/apis/proxies/v1alpha1"
	schedulerapi "github.com/clusternet/clusternet/pkg/apis/scheduler"
	"github.com/clusternet/clusternet/pkg/features"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/names"
	"github.com/clusternet/clusternet/pkg/utils"
)

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.Predictor

var _ framework.PredictPlugin = &Predictor{}
var _ framework.PrePredictPlugin = &Predictor{}

// Predictor is a plugin than checks if a subscription need resources
type Predictor struct {
	handle framework.Handle
}

type ClusterTopology struct {
	subscribers []appsapi.Subscriber
}

func (ct *ClusterTopology) Clone() framework.StateData {
	return ct
}

func getClusterTopology(cycleState *framework.CycleState, clusterName string) []appsapi.Subscriber {
	c, err := cycleState.Read(framework.StateKey(clusterName))
	if err != nil {
		klog.Warningf("read cluster %s topology data from cycle state err : %v", clusterName, err)
		return nil
	}
	data, ok := c.(*ClusterTopology)
	if !ok {
		klog.Warningf("get cluster %s topology data from state data not ok ", clusterName)
	}
	return data.subscribers
}

// New initializes a new plugin and returns it.
func New(_ runtime.Object, h framework.Handle) (framework.Plugin, error) {
	return &Predictor{handle: h}, nil
}

// Name returns name of the plugin. It is used in logs, etc.
func (pl *Predictor) Name() string {
	return Name
}

func (pl *Predictor) PrePredict(ctx context.Context,
	state *framework.CycleState,
	sub *appsapi.Subscription,
	finv *appsapi.FeedInventory,
	clusters []*clusterapi.ManagedCluster) (s *framework.Status) {

	if sub.Spec.DividingScheduling.Type != appsapi.DynamicReplicaDividingType || !metav1.HasAnnotation(sub.ObjectMeta, known.AnnoEnableTopology) {
		klog.V(5).Infof("subs %s/%s not dynamic dividing schedule or not enable topology , skip plugin %s ", sub.Namespace, sub.Name, pl.Name())
		return framework.NewStatus(framework.Skip, "not dynamic topology schedule")
	}

	klog.V(5).Infof("subs %s/%s use dynamic topology schedule, will set all topology predict data ... ", sub.Namespace, sub.Name)
	for _, mcls := range clusters {
		var domain = &ClusterTopology{}
		for k, v := range mcls.Labels {
			if utils.IsZoneKey(k) {
				domain.subscribers = append(domain.subscribers, appsapi.Subscriber{ClusterAffinity: &metav1.LabelSelector{MatchLabels: map[string]string{k: v}}})
				continue
			}
		}
		state.Write(framework.StateKey(mcls.Name), domain)
	}
	return framework.NewStatus(framework.Success)
}

// Predict invoked by scheduler predictor plugin
func (pl *Predictor) Predict(_ context.Context,
	state *framework.CycleState,
	subs *appsapi.Subscription,
	finv *appsapi.FeedInventory,
	mcls *clusterapi.ManagedCluster) (
	feedReplicas framework.FeedReplicas,
	s *framework.Status) {
	if !mcls.Status.PredictorEnabled {
		klog.V(5).Infof("subs %s/%s run predictor for cluster %s will skip , because cluster predictor isdisabled ",
			subs.Namespace, subs.Name, mcls.Name)
		return nil, framework.NewStatus(framework.Skip, "predictor is disabled")
	}

	klog.V(5).Infof("subs %s/%s will run predictor for cluster %s ... ", subs.Namespace, subs.Name, mcls.Name)
	predictorAddress := mcls.Status.PredictorAddress
	httpClient := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		},
	}
	var err error
	if !mcls.Status.PredictorDirectAccess {
		klog.V(5).Infof("the predictor of cluster %s can not be accessed directly", mcls.Spec.ClusterID)
		if !mcls.Status.UseSocket {
			return nil, framework.AsStatus(fmt.Errorf("cluster %s does not enable feature gate %s",
				mcls.Spec.ClusterID, features.SocketConnection))
		}

		httpClient, err = restclient.HTTPClientFor(pl.handle.KubeConfig())
		if err != nil {
			return nil, framework.AsStatus(err)
		}

		predictorAddress = strings.Replace(predictorAddress, "http://", "http/", 1)
		predictorAddress = strings.Replace(predictorAddress, "https://", "https/", 1)
		predictorAddress = strings.Join([]string{
			strings.TrimRight(pl.handle.KubeConfig().Host, "/"),
			fmt.Sprintf(
				"apis/%s/sockets/%s/proxy/%s",
				proxiesapi.SchemeGroupVersion.String(),
				mcls.Spec.ClusterID,
				predictorAddress,
			),
		}, "/")
	}
	httpClient.Timeout = time.Second * 32

	for _, feedOrder := range finv.Spec.Feeds {
		if feedOrder.DesiredReplicas == nil {
			feedReplicas = append(feedReplicas, nil)
			continue
		}

		var require = schedulerapi.PredictorRequire{
			Requirements: feedOrder.ReplicaRequirements,
		}
		if metav1.HasAnnotation(subs.ObjectMeta, known.AnnoEnableTopology) {
			switch subs.Spec.DividingScheduling.Type {
			case appsapi.DynamicReplicaDividingType:
				require.Topologys = getClusterTopology(state, mcls.Name)
			case appsapi.StaticReplicaDividingType:
				require.Topologys = subs.Spec.Subscribers
			default:
				klog.Info("enable topology schedule , but not set schedule type , skip topology data ... ")
			}
		}
		replica, err2 := predictMaxAcceptableReplicas(httpClient,
			mcls.Spec.ClusterID,
			predictorAddress,
			require)
		if err2 != nil {
			//feedReplicas = append(feedReplicas, map[string]int32{"default": 0})
			feedReplicas = append(feedReplicas, map[string]int32{})
			klog.Warningf("subs %s/%s request cluster %s predictor %s get basic 1 replica with error : %v",
				subs.Namespace, subs.Name, mcls.Name, predictorAddress, err2)
			//return nil, framework.AsStatus(err2)
		} else {
			// Todo : support topology-aware replicas
			feedReplicas = append(feedReplicas, replica)
		}
	}
	return
}

func predictMaxAcceptableReplicas(httpClient *http.Client,
	clusterID types.UID, address string,
	require schedulerapi.PredictorRequire) (map[string]int32, error) {
	payload, err := json.Marshal(require)
	if err != nil {
		return nil, err
	}

	urlForReplicasPredicting, err := url.JoinPath(address, schedulerapi.RootPathReplicas, schedulerapi.SubPathPredict)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequest(http.MethodPost, urlForReplicasPredicting, bytes.NewBuffer(payload))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(schedulerapi.ClusterIDHeader, string(clusterID))

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		klog.Warningf("error of do request with code %d and message %s", resp.StatusCode, resp.Status)
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	predictorResults := &schedulerapi.PredictorResults{}
	err = json.Unmarshal(data, predictorResults)
	if err != nil {
		return nil, err
	}

	return predictorResults.Replicas, nil
}
