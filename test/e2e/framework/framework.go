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
	"path/filepath"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	cs "kubedb.dev/apimachinery/client/clientset/versioned"
	test_util "kubedb.dev/redis/pkg/testing"

	cm "github.com/jetstack/cert-manager/pkg/client/clientset/versioned"
	. "github.com/onsi/gomega"
	"gomodules.xyz/blobfs"
	"gomodules.xyz/cert/certstore"
	"gomodules.xyz/x/crypto/rand"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ka "k8s.io/kube-aggregator/pkg/client/clientset_generated/clientset"
	appcat_cs "kmodules.xyz/custom-resources/client/clientset/versioned/typed/appcatalog/v1alpha1"
)

var (
	DockerRegistry = "kubedbci"
	//DBCatalogName  = "5.0.3-v1"
	DBCatalogName = "6.0.6"
	Cluster       = true
	WithTLSConfig = true
)

type Framework struct {
	restConfig        *rest.Config
	KubeClient        kubernetes.Interface
	dbClient          cs.Interface
	kaClient          ka.Interface
	dmClient          dynamic.Interface
	appCatalogClient  appcat_cs.AppcatalogV1alpha1Interface
	namespace         string
	name              string
	StorageClass      string
	CertStore         *certstore.CertStore
	certManagerClient cm.Interface
	testConfig        *test_util.TestConfig
}

func New(
	restConfig *rest.Config,
	kubeClient kubernetes.Interface,
	extClient cs.Interface,
	kaClient ka.Interface,
	dmClient dynamic.Interface,
	appCatalogClient appcat_cs.AppcatalogV1alpha1Interface,
	certManagerClient cm.Interface,
	storageClass string,
) *Framework {
	store, err := certstore.New(blobfs.NewInMemory(), filepath.Join("", "pki"))
	Expect(err).NotTo(HaveOccurred())

	err = store.InitCA()
	Expect(err).NotTo(HaveOccurred())

	testConfig := &test_util.TestConfig{
		RestConfig:    restConfig,
		KubeClient:    kubeClient,
		DBCatalogName: DBCatalogName,
		WithTLS:       WithTLSConfig,
	}
	return &Framework{
		testConfig:        testConfig,
		restConfig:        restConfig,
		KubeClient:        kubeClient,
		dbClient:          extClient,
		kaClient:          kaClient,
		dmClient:          dmClient,
		appCatalogClient:  appCatalogClient,
		certManagerClient: certManagerClient,
		name:              "redis-operator",
		namespace:         rand.WithUniqSuffix(api.ResourceSingularRedis),
		StorageClass:      storageClass,
		CertStore:         store,
	}
}

func (f *Framework) Invoke() *Invocation {
	return &Invocation{
		Framework: f,
		app:       rand.WithUniqSuffix("redis-e2e"),
	}
}

func (fi *Invocation) ExtClient() cs.Interface {
	return fi.dbClient
}

func (fi *Invocation) RestConfig() *rest.Config {
	return fi.restConfig
}
func (fi *Invocation) TestConfig() *test_util.TestConfig {
	return fi.testConfig
}

type Invocation struct {
	*Framework
	app           string
	TestResources []interface{}
}
