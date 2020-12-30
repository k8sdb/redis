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

package admission

import (
	"context"
	"net/http"
	"testing"

	catalog "kubedb.dev/apimachinery/apis/catalog/v1alpha1"
	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	extFake "kubedb.dev/apimachinery/client/clientset/versioned/fake"
	"kubedb.dev/apimachinery/client/clientset/versioned/scheme"

	"gomodules.xyz/pointer"
	admission "k8s.io/api/admission/v1beta1"
	authenticationV1 "k8s.io/api/authentication/v1"
	core "k8s.io/api/core/v1"
	storageV1beta1 "k8s.io/api/storage/v1beta1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metaV1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientSetScheme "k8s.io/client-go/kubernetes/scheme"
	core_util "kmodules.xyz/client-go/core/v1"
	meta_util "kmodules.xyz/client-go/meta"
	mona "kmodules.xyz/monitoring-agent-api/api/v1"
)

func init() {
	utilruntime.Must(scheme.AddToScheme(clientSetScheme.Scheme))
}

var requestKind = metaV1.GroupVersionKind{
	Group:   api.SchemeGroupVersion.Group,
	Version: api.SchemeGroupVersion.Version,
	Kind:    api.ResourceKindRedis,
}

func TestRedisValidator_Admit(t *testing.T) {
	for _, c := range cases {
		t.Run(c.testName, func(t *testing.T) {
			validator := RedisValidator{
				ClusterTopology: &core_util.Topology{},
			}

			validator.initialized = true
			validator.extClient = extFake.NewSimpleClientset(
				&catalog.RedisVersion{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "4.0",
					},
				},
			)
			validator.client = fake.NewSimpleClientset(
				&storageV1beta1.StorageClass{
					ObjectMeta: metaV1.ObjectMeta{
						Name: "standard",
					},
				},
			)

			objJS, err := meta_util.MarshalToJson(&c.object, api.SchemeGroupVersion)
			if err != nil {
				panic(err)
			}
			oldObjJS, err := meta_util.MarshalToJson(&c.oldObject, api.SchemeGroupVersion)
			if err != nil {
				panic(err)
			}

			req := new(admission.AdmissionRequest)

			req.Kind = c.kind
			req.Name = c.objectName
			req.Namespace = c.namespace
			req.Operation = c.operation
			req.UserInfo = authenticationV1.UserInfo{}
			req.Object.Raw = objJS
			req.OldObject.Raw = oldObjJS

			if c.heatUp {
				if _, err := validator.extClient.KubedbV1alpha2().Redises(c.namespace).Create(context.TODO(), &c.object, metaV1.CreateOptions{}); err != nil && !kerr.IsAlreadyExists(err) {
					t.Errorf(err.Error())
				}
			}
			if c.operation == admission.Delete {
				req.Object = runtime.RawExtension{}
			}
			if c.operation != admission.Update {
				req.OldObject = runtime.RawExtension{}
			}

			response := validator.Admit(req)
			if c.result {
				if response.Allowed != true {
					t.Errorf("expected: 'Allowed=true'. but got response: %v", response)
				}
			} else {
				if response.Allowed || response.Result.Code == http.StatusInternalServerError {
					t.Errorf("expected: 'Allowed=false', but got response: %v", response)
				}
			}
		})
	}

}

var cases = []struct {
	testName   string
	kind       metaV1.GroupVersionKind
	objectName string
	namespace  string
	operation  admission.Operation
	object     api.Redis
	oldObject  api.Redis
	heatUp     bool
	result     bool
}{
	{"Create Valid Redis",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleRedis(),
		api.Redis{},
		false,
		true,
	},
	{"Create Invalid Redis (invalid version)",
		requestKind,
		"foo",
		"default",
		admission.Create,
		getAwkwardRedis(),
		api.Redis{},
		false,
		false,
	},
	{"Create Invalid Redis (invalid replicas)",
		requestKind,
		"foo",
		"default",
		admission.Create,
		getAwkwardRedisWithInvalidReplicas(),
		api.Redis{},
		false,
		false,
	},
	{"Edit Redis Spec.Version",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecVersion(sampleRedis()),
		sampleRedis(),
		false,
		false,
	},
	{"Edit Status",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editStatus(sampleRedis()),
		sampleRedis(),
		false,
		true,
	},
	{"Edit Spec.Monitor",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecMonitor(sampleRedis()),
		sampleRedis(),
		false,
		true,
	},
	{"Edit Invalid Spec.Monitor",
		requestKind,
		"foo",
		"default",
		admission.Update,
		editSpecInvalidMonitor(sampleRedis()),
		sampleRedis(),
		false,
		false,
	},
	{"Edit Spec.TerminationPolicy",
		requestKind,
		"foo",
		"default",
		admission.Update,
		haltDatabase(sampleRedis()),
		sampleRedis(),
		false,
		true,
	},
	{"Delete Redis when Spec.TerminationPolicy = DoNotTerminate",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		sampleRedis(),
		api.Redis{},
		true,
		false,
	},
	{"Delete Redis when Spec.TerminationPolicy = Halt",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		haltDatabase(sampleRedis()),
		api.Redis{},
		true,
		true,
	},
	{"Delete Non Existing Redis",
		requestKind,
		"foo",
		"default",
		admission.Delete,
		api.Redis{},
		api.Redis{},
		false,
		true,
	},

	// For Redis Cluster
	{"Create Valid Cluster",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleCluster(),
		api.Redis{},
		false,
		true,
	},
	{"Create Invalid Cluster (invalid Spec.Cluster.Master)",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleClusterWithInvalidMaster(),
		api.Redis{},
		false,
		false,
	},
	{"Create Invalid Cluster (invalid Spec.Cluster.Replicas)",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleClusterWithInvalidReplicas(),
		api.Redis{},
		false,
		false,
	},
	{"Create Invalid Cluster (invalid Spec.Mode)",
		requestKind,
		"foo",
		"default",
		admission.Create,
		sampleRedisWithInvalidMode(),
		api.Redis{},
		false,
		false,
	},
	{"Edit spec.Init before provisioning complete",
		requestKind,
		"foo",
		"default",
		admission.Update,
		updateInit(sampleRedis()),
		sampleRedis(),
		true,
		true,
	},
	{"Edit spec.Init after provisioning complete",
		requestKind,
		"foo",
		"default",
		admission.Update,
		updateInit(completeInitialization(sampleRedis())),
		completeInitialization(sampleRedis()),
		true,
		false,
	},
}

func sampleRedisWithoutMode() api.Redis {
	return api.Redis{
		TypeMeta: metaV1.TypeMeta{
			Kind:       api.ResourceKindRedis,
			APIVersion: api.SchemeGroupVersion.String(),
		},
		ObjectMeta: metaV1.ObjectMeta{
			Name:      "foo",
			Namespace: "default",
			Labels: map[string]string{
				meta_util.NameLabelKey: api.Redis{}.ResourceFQN(),
			},
		},
		Spec: api.RedisSpec{
			Version:     "4.0",
			Replicas:    pointer.Int32P(1),
			StorageType: api.StorageTypeDurable,
			Storage: &core.PersistentVolumeClaimSpec{
				StorageClassName: pointer.StringP("standard"),
				Resources: core.ResourceRequirements{
					Requests: core.ResourceList{
						core.ResourceStorage: resource.MustParse("100Mi"),
					},
				},
			},
			TerminationPolicy: api.TerminationPolicyDoNotTerminate,
			Init: &api.InitSpec{
				WaitForInitialRestore: true,
			},
		},
	}
}

func sampleRedisWithInvalidMode() api.Redis {
	redis := sampleRedisWithoutMode()
	redis.Spec.Mode = api.RedisMode("cluster")

	return redis
}

func sampleRedis() api.Redis {
	redis := sampleRedisWithoutMode()
	redis.Spec.Mode = api.RedisModeStandalone

	return redis
}

func getAwkwardRedis() api.Redis {
	redis := sampleRedis()
	redis.Spec.Version = "3.0"
	return redis
}

func getAwkwardRedisWithInvalidReplicas() api.Redis {
	redis := sampleRedis()
	redis.Spec.Replicas = pointer.Int32P(3)
	return redis
}

func editSpecVersion(old api.Redis) api.Redis {
	old.Spec.Version = "4.4"
	return old
}

func editStatus(old api.Redis) api.Redis {
	old.Status = api.RedisStatus{
		Phase: api.DatabasePhaseReady,
	}
	return old
}

func editSpecMonitor(old api.Redis) api.Redis {
	old.Spec.Monitor = &mona.AgentSpec{
		Agent: mona.AgentPrometheusBuiltin,
		Prometheus: &mona.PrometheusSpec{
			Exporter: mona.PrometheusExporterSpec{
				Port: 4567,
			},
		},
	}
	return old
}

// should be failed because more fields required for COreOS Monitoring
func editSpecInvalidMonitor(old api.Redis) api.Redis {
	old.Spec.Monitor = &mona.AgentSpec{
		Agent: mona.AgentPrometheusOperator,
	}
	return old
}

func haltDatabase(old api.Redis) api.Redis {
	old.Spec.TerminationPolicy = api.TerminationPolicyHalt
	return old
}

// For Redis Cluster
func sampleClusterWithOnlyMode() api.Redis {
	redis := sampleRedis()
	redis.Spec.Mode = api.RedisModeCluster

	return redis
}

func sampleCluster() api.Redis {
	redis := sampleClusterWithOnlyMode()
	redis.Spec.Cluster = &api.RedisClusterSpec{
		Master:   pointer.Int32P(3),
		Replicas: pointer.Int32P(1),
	}

	return redis
}

func sampleClusterWithInvalidMaster() api.Redis {
	redis := sampleClusterWithOnlyMode()
	redis.Spec.Cluster = &api.RedisClusterSpec{
		Master:   pointer.Int32P(2),
		Replicas: pointer.Int32P(1),
	}

	return redis
}

func sampleClusterWithInvalidReplicas() api.Redis {
	redis := sampleClusterWithOnlyMode()
	redis.Spec.Cluster = &api.RedisClusterSpec{
		Master:   pointer.Int32P(3),
		Replicas: pointer.Int32P(0),
	}

	return redis
}

func completeInitialization(old api.Redis) api.Redis {
	old.Spec.Init.Initialized = true
	return old
}

func updateInit(old api.Redis) api.Redis {
	old.Spec.Init.WaitForInitialRestore = false
	return old
}
