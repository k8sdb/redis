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
	"fmt"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	"github.com/pkg/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	core_util "kmodules.xyz/client-go/core/v1"
)

func (f *Framework) WaitUntilStatefulSetReady(redis *api.Redis) error {
	for i := 0; i < int(*redis.Spec.Cluster.Master); i++ {
		for j := 0; j <= int(*redis.Spec.Cluster.Replicas); j++ {
			podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, i, j)
			err := core_util.WaitUntilPodRunning(
				context.TODO(),
				f.KubeClient,
				metav1.ObjectMeta{
					Name:      podName,
					Namespace: redis.Namespace,
				},
			)
			if err != nil {
				return errors.Wrapf(err, "failed to ready pod '%s/%s'", redis.Namespace, podName)
			}
		}
	}

	return nil
}
