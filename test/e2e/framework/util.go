/*
Copyright AppsCode Inc. and Contributors

Licensed under the AppsCode Community License 1.0.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://github.com/appscode/licenses/raw/1.0.0/AppsCode-Community-1.0.0.md

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/apimachinery/apis/ops/v1alpha1"
	opsapi "kubedb.dev/apimachinery/apis/ops/v1alpha1"

	shell "github.com/codeskyblue/go-sh"
	cm_api "github.com/jetstack/cert-manager/pkg/apis/certmanager/v1alpha2"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
)

const (
	updateRetryInterval = 10 * 1000 * 1000 * time.Nanosecond
	maxAttempts         = 5
	PullInterval        = time.Second * 2
	WaitTimeOut         = time.Minute * 3
)

func deleteInBackground() *metav1.DeleteOptions {
	policy := metav1.DeletePropagationBackground
	return &metav1.DeleteOptions{PropagationPolicy: &policy}
}
func deleteInForeground() metav1.DeleteOptions {
	policy := metav1.DeletePropagationForeground
	return metav1.DeleteOptions{PropagationPolicy: &policy}
}

func (fi *Invocation) GetPod(meta metav1.ObjectMeta) (*core.Pod, error) {
	podList, err := fi.KubeClient.CoreV1().Pods(meta.Namespace).List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	for _, pod := range podList.Items {
		if bytes.HasPrefix([]byte(pod.Name), []byte(meta.Name)) {
			return &pod, nil
		}
	}
	return nil, fmt.Errorf("no pod found for workload %v", meta.Name)
}

func (fi *Invocation) GetCustomConfig(configs []string) *core.Secret {
	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fi.app,
			Namespace: fi.namespace,
		},
		StringData: map[string]string{
			"redis.conf": strings.Join(configs, "\n"),
		},
	}
}

func (fi *Invocation) CreateConfigMap(obj *core.ConfigMap) error {
	_, err := fi.KubeClient.CoreV1().ConfigMaps(obj.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
	return err
}

func (fi *Invocation) DeleteConfigMap(meta metav1.ObjectMeta) error {
	err := fi.KubeClient.CoreV1().ConfigMaps(meta.Namespace).Delete(context.TODO(), meta.Name, deleteInForeground())
	if err != nil && !kerr.IsNotFound(err) {
		return err
	}
	return nil
}

func (f *Framework) EventuallyWipedOut(meta metav1.ObjectMeta) GomegaAsyncAssertion {
	return Eventually(
		func() error {
			labelMap := map[string]string{
				api.LabelDatabaseName: meta.Name,
				api.LabelDatabaseKind: api.ResourceKindRedis,
			}
			labelSelector := labels.SelectorFromSet(labelMap)

			// check if pvcs is wiped out
			pvcList, err := f.KubeClient.CoreV1().PersistentVolumeClaims(meta.Namespace).List(
				context.TODO(),
				metav1.ListOptions{
					LabelSelector: labelSelector.String(),
				},
			)
			if err != nil {
				return err
			}
			if len(pvcList.Items) > 0 {
				return fmt.Errorf("PVCs have not wiped out yet")
			}

			// check if secrets are wiped out
			secretList, err := f.KubeClient.CoreV1().Secrets(meta.Namespace).List(
				context.TODO(),
				metav1.ListOptions{
					LabelSelector: labelSelector.String(),
				},
			)
			if err != nil {
				return err
			}
			if len(secretList.Items) > 0 {
				return fmt.Errorf("secrets have not wiped out yet")
			}

			// check if appbinds are wiped out
			appBindingList, err := f.appCatalogClient.AppBindings(meta.Namespace).List(
				context.TODO(),
				metav1.ListOptions{
					LabelSelector: labelSelector.String(),
				},
			)
			if err != nil {
				return err
			}
			if len(appBindingList.Items) > 0 {
				return fmt.Errorf("appBindings have not wiped out yet")
			}

			return nil
		},
		time.Minute*5,
		time.Second*5,
	)
}
func (fi *Invocation) AppendToCleanupList(resources ...interface{}) {

	for r := range resources {
		//fmt.Println(resources[0])
		fi.TestResources = append(fi.TestResources, resources[r])
	}
}

func (fi *Invocation) CleanupTestResources() error {
	// delete all test resources
	By("Cleaning Test Resources")
	if fi.TestResources == nil {

		return nil
	}
	for r := range fi.TestResources {
		gvr, objMeta, err := getGVRAndObjectMeta(fi.TestResources[r])
		if err != nil {
			return err
		}

		err = fi.dmClient.Resource(gvr).Namespace(objMeta.Namespace).Delete(context.TODO(), objMeta.Name, *deleteInBackground())

		if err != nil && !kerr.IsNotFound(err) {
			return err
		}
	}

	// wait until resource has been deleted
	for r := range fi.TestResources {
		gvr, objMeta, err := getGVRAndObjectMeta(fi.TestResources[r])
		if err != nil {
			return err
		}
		err = fi.waitUntilResourceDeleted(gvr, objMeta)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *Framework) waitUntilResourceDeleted(gvr schema.GroupVersionResource, objMeta metav1.ObjectMeta) error {
	return wait.PollImmediate(PullInterval, WaitTimeOut, func() (done bool, err error) {
		if _, err := f.dmClient.Resource(gvr).Namespace(objMeta.Namespace).Get(context.TODO(), objMeta.Name, metav1.GetOptions{}); err != nil {
			if kerr.IsNotFound(err) {
				return true, nil
			} else {
				return true, err
			}
		}
		return false, nil
	})
}

func getGVRAndObjectMeta(obj interface{}) (schema.GroupVersionResource, metav1.ObjectMeta, error) {
	switch w := obj.(type) {
	case *api.Redis:
		w.GetObjectKind().SetGroupVersionKind(api.SchemeGroupVersion.WithKind(api.ResourceKindRedis))
		gvk := w.GroupVersionKind()
		return schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: api.ResourceKindRedis}, w.ObjectMeta, nil
	case *v1alpha1.RedisOpsRequest:
		w.GetObjectKind().SetGroupVersionKind(opsapi.SchemeGroupVersion.WithKind(opsapi.ResourceKindRedisOpsRequest))
		gvk := w.GroupVersionKind()
		return schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: opsapi.ResourcePluralRedisOpsRequest}, w.ObjectMeta, nil
	case *core.Secret:
		w.GetObjectKind().SetGroupVersionKind(core.SchemeGroupVersion.WithKind("Secret"))
		gvk := w.GroupVersionKind()
		return schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: "secrets"}, w.ObjectMeta, nil
	case *cm_api.Issuer:
		w.GetObjectKind().SetGroupVersionKind(cm_api.SchemeGroupVersion.WithKind(cm_api.IssuerKind))
		gvk := w.GroupVersionKind()
		return schema.GroupVersionResource{Group: gvk.Group, Version: gvk.Version, Resource: "issuers"}, w.ObjectMeta, nil
	default:
		return schema.GroupVersionResource{}, metav1.ObjectMeta{}, fmt.Errorf("failed to get GroupVersionResource. Reason: Unknown resource type")
	}
}

func (f *Framework) PrintDebugHelpers() {
	sh := shell.NewSession()

	fmt.Println("\n======================================[ Describe Pod ]===================================================")
	if err := sh.Command("/usr/bin/kubectl", "describe", "po", "-n", f.Namespace()).Run(); err != nil {
		fmt.Println(err)
	}
	fmt.Println("\n======================================[ Describe Redis ]===================================================")
	if err := sh.Command("/usr/bin/kubectl", "describe", "rd", "-n", f.Namespace()).Run(); err != nil {
		fmt.Println(err)
	}
	fmt.Println("\n======================================[ Describe Nodes ]===================================================")
	if err := sh.Command("/usr/bin/kubectl", "describe", "nodes").Run(); err != nil {
		fmt.Println(err)
	}
}
