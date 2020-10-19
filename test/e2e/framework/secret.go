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
	"context"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	meta_util "kmodules.xyz/client-go/meta"
)

func (f *Framework) CreateSecret(obj *core.Secret) (*core.Secret, error) {
	return f.KubeClient.CoreV1().Secrets(obj.Namespace).Create(context.TODO(), obj, metav1.CreateOptions{})
}

func (f *Framework) DeleteSecret(meta metav1.ObjectMeta) error {
	return f.KubeClient.CoreV1().Secrets(meta.Namespace).Delete(context.TODO(), meta.Name, meta_util.DeleteInForeground())
}

func (f *Framework) SelfSignedCASecret(meta metav1.ObjectMeta, kind string) *core.Secret {
	labelMap := map[string]string{
		api.LabelDatabaseName: meta.Name,
		api.LabelDatabaseKind: kind,
	}
	return &core.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      meta.Name + "-self-signed-ca",
			Namespace: meta.Namespace,
			Labels:    labelMap,
		},
		Data: map[string][]byte{
			"tls.crt": f.CertStore.CACertBytes(),
			"tls.key": f.CertStore.CAKeyBytes(),
		},
	}
}
