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
	"strings"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (f *Framework) GetDatabasePod(meta metav1.ObjectMeta) (*core.Pod, error) {
	return f.KubeClient.CoreV1().Pods(meta.Namespace).Get(context.TODO(), meta.Name+"-0", metav1.GetOptions{})
}

func (f *Framework) EventuallyRedisConfig(redis *api.Redis, config string) GomegaAsyncAssertion {
	configPair := strings.Split(config, " ")

	return Eventually(
		func() string {
			// ping database to check if it is ready
			pong, err := f.testConfig.GetPingResult(redis)
			if err != nil {
				return ""
			}

			if !strings.Contains(pong, "PONG") {
				return ""
			}

			// get configuration
			result, err := f.testConfig.GetRedisConfig(redis, configPair[0])
			if err != nil {
				return ""
			}

			return strings.Join(result, " ")
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) EventuallySetItem(redis *api.Redis, key, value string) GomegaAsyncAssertion {
	return Eventually(
		func() bool {
			_, err := f.testConfig.SetItem(redis, key, value)
			return err == nil
		},
		time.Minute*5,
		time.Second*5,
	)
}

func (f *Framework) EventuallyGetItem(redis *api.Redis, key string) GomegaAsyncAssertion {
	return Eventually(
		func() string {
			res, err := f.testConfig.GetItem(redis, key)
			if err != nil {
				return ""
			}
			return res
		},
		time.Minute*5,
		time.Second*5,
	)
}
