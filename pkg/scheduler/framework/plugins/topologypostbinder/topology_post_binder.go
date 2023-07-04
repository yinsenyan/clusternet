package topologypostbinder

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	jsonpatch "github.com/evanphx/json-patch"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	appsapi "github.com/clusternet/clusternet/pkg/apis/apps/v1alpha1"
	"github.com/clusternet/clusternet/pkg/known"
	framework "github.com/clusternet/clusternet/pkg/scheduler/framework/interfaces"
	"github.com/clusternet/clusternet/pkg/scheduler/framework/plugins/names"
	"github.com/clusternet/clusternet/pkg/utils"
)

const AnnoTopologyReplicas = "topology.camp.io/replicas"

// Name is the name of the plugin used in the plugin registry and configurations.
const Name = names.TopologyPostBinder

// TopologyBinder binds subscriptions and workload to cluster topologys by annotation and localizatoins using a clusternet client.
type TopologyPostBinder struct {
	handle framework.Handle
}

var _ framework.PostBindPlugin = &TopologyPostBinder{}

// New creates a TopologyBinder.
func New(_ runtime.Object, handle framework.Handle) (framework.Plugin, error) {
	return &TopologyPostBinder{handle: handle}, nil
}

// Name returns the name of the plugin.
func (pl *TopologyPostBinder) Name() string {
	return Name
}

// Bind binds subscriptions and workload to cluster topologys by annotation and localizatoins using a clusternet client.
func (pl *TopologyPostBinder) PostBind(ctx context.Context,
	state *framework.CycleState,
	sub *appsapi.Subscription,
	targetClusters framework.TargetClusters) {
	// There is two jobs
	// 1. patch all topology data to subs annotation
	// 2. create/update localization which include this cluster topology data for every workload

	klog.V(3).InfoS("Attempting run topology postbind subscription topology data",
		"subscription", klog.KObj(sub),
		"topology data is", targetClusters)

	subCopy := sub.DeepCopy()
	if !metav1.HasAnnotation(subCopy.ObjectMeta, known.AnnoEnableTopology) {
		klog.V(3).InfoS("Skip topology postbind subscription topology data with not enable topology scheduling",
			"subscription", klog.KObj(sub))
		return
	}

	var err error
	err = pl.PatchSubs(ctx, *sub, targetClusters)
	if err != nil {
		klog.Warningf("warning of sub %s/%s patch topology data error : %v ", sub.Namespace, sub.Name, targetClusters)
	}
	err = pl.PatchLocal(ctx, *sub, targetClusters)
	if err != nil {
		klog.Warningf("warning of sub %s/%s patch topology data local error : %v ", sub.Namespace, sub.Name, targetClusters)
	}
	//return
}

func (pl *TopologyPostBinder) PatchSubs(ctx context.Context,
	sub appsapi.Subscription,
	targetClusters framework.TargetClusters) error {

	oldData, err := json.Marshal(sub)
	if err != nil {
		return fmt.Errorf("failed to marshal original subscription: %v", err)
	}
	subCopy := sub.DeepCopy()

	subCopy.Status.BindingClusters = targetClusters.BindingClusters
	subCopy.Status.Replicas = targetClusters.Replicas
	subCopy.Status.SpecHash = utils.HashSubscriptionSpec(&subCopy.Spec)
	subCopy.Status.DesiredReleases = len(targetClusters.BindingClusters)

	subAnnoCopy := sub.DeepCopy()
	r, err := json.Marshal(targetClusters)
	if err != nil {
		return fmt.Errorf("failed to marshal target cluster for subscription: %v", err)
	}
	subAnnoCopy.ObjectMeta.Annotations[known.AnnoTopologyReplicas] = string(r)

	annoData, err := json.Marshal(subAnnoCopy)
	if err != nil {
		return fmt.Errorf("failed to marshal new subscription: %v", err)
	}

	annoBytes, err := jsonpatch.CreateMergePatch(oldData, annoData)
	if err != nil {
		return fmt.Errorf("failed to create a merge patch: %v", err)
	}
	klog.V(5).Infof("subs %s/%s patch bytes is : %s", sub.Namespace, sub.Name, string(annoBytes))
	_, err = pl.handle.ClientSet().AppsV1alpha1().
		Subscriptions(sub.Namespace).
		Patch(context.TODO(), sub.Name, types.MergePatchType, annoBytes, metav1.PatchOptions{})
	if err != nil {
		return fmt.Errorf("subs %s/%s patch err is : %v", sub.Namespace, sub.Name, err)
	}
	return nil
}

func (pl *TopologyPostBinder) PatchLocal(ctx context.Context,
	sub appsapi.Subscription,
	targetClusters framework.TargetClusters) (err error) {

	localizations := generatorLocal(sub.Name, targetClusters)
	for _, local := range localizations {
		// TODO add base uid to local ownerrefence
		// local.SetOwnerReferences([]metav1.OwnerReference{
		// 	{
		// 		APIVersion: "apps.clusternet.io/v1alpha1",
		// 		Kind:       "Base",
		// 		Name:       fmt.Sprintf("%s-%s", sub.Namespace, sub.Name),
		//		Uid: "",
		// 	},
		// })
		klog.V(5).Infof("sub %s/%s will sync topology local %s",
			sub.Namespace, sub.Name, local.Name)
		err = pl.syncLocalization(ctx, local)
		if err != nil {
			klog.Warningf("sync subscription %s/%s topology localization %s/%s with error : %v",
				sub.Namespace, sub.Name, local.Namespace, local.Name, err)
		}
	}
	return
}

func (pl *TopologyPostBinder) syncLocalization(ctx context.Context, local appsapi.Localization) (err error) {
	_, err = pl.handle.ClientSet().
		AppsV1alpha1().Localizations(local.Namespace).
		Create(ctx, &local, metav1.CreateOptions{})
	if err == nil {
		return nil
	} else {
		if errors.IsAlreadyExists(err) {
			curLocal, err := pl.handle.ClientSet().AppsV1alpha1().Localizations(local.Namespace).Get(ctx, local.Name, metav1.GetOptions{})
			if err != nil {
				klog.Warning("get local err : ", err)
			}
			local.ResourceVersion = curLocal.ResourceVersion
			_, err = pl.handle.ClientSet().
				AppsV1alpha1().
				Localizations(local.Namespace).
				Update(ctx, &local, metav1.UpdateOptions{})
			if err != nil {
				return err
			}
			return nil
		}
		return err
	}
}

func generatorLocal(subName string, targetClusters framework.TargetClusters) []appsapi.Localization {
	var locals []appsapi.Localization
	for i, cluster := range targetClusters.BindingClusters {
		ns, _, err := cache.SplitMetaNamespaceKey(cluster)
		if err != nil {
			return nil
		}

		for feedKey, replicas := range targetClusters.Replicas {
			if len(replicas) == 0 {
				continue
			}

			feed := utils.GetFeed(feedKey)
			topologyData := targetClusters.TopologyReplicas[i].FeedReplicas[feedKey]
			data, err := json.Marshal(topologyData)
			if err != nil {
				return nil
			}
			path := "/metadata/annotations/" + utils.EscapeJSONPointerValue(AnnoTopologyReplicas)
			local := appsapi.Localization{
				ObjectMeta: metav1.ObjectMeta{
					Name:      fmt.Sprintf("tp-%s-%s-%s-%s", subName, strings.ToLower(feed.Kind), feed.Namespace, feed.Name),
					Namespace: ns,
				},
				Spec: appsapi.LocalizationSpec{
					OverridePolicy: appsapi.ApplyNow,
					Overrides: []appsapi.OverrideConfig{
						{
							Name: "topology",
							Type: appsapi.JSONPatchType,
							Value: fmt.Sprintf(`[{"op":"replace","path":"%s","value":%q}]`,
								path, string(data)),
						},
					},
					Feed: feed,
				},
			}
			locals = append(locals, local)
		}
	}
	return locals
}
