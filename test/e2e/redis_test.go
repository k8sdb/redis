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

package e2e_test

import (
	"context"
	"fmt"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"
	"kubedb.dev/apimachinery/client/clientset/versioned/typed/kubedb/v1alpha2/util"
	"kubedb.dev/redis/test/e2e/framework"
	"kubedb.dev/redis/test/e2e/matcher"

	"github.com/appscode/go/crypto/rand"
	"github.com/appscode/go/log"
	"github.com/appscode/go/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	core "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	exec_util "kmodules.xyz/client-go/tools/exec"
)

var _ = Describe("Redis", func() {
	var (
		err         error
		f           *framework.Invocation
		redis       *api.Redis
		skipMessage string
		key         string
		value       string
	)

	BeforeEach(func() {
		f = root.Invoke()
		redis = f.Redis()
		if framework.WithTLSConfig {
			redis = f.RedisWithTLS(redis)
		}
		skipMessage = ""
		key = rand.WithUniqSuffix("kubed-e2e")
		value = rand.GenerateTokenWithLength(10)
	})

	JustAfterEach(func() {
		if CurrentGinkgoTestDescription().Failed {
			f.PrintDebugHelpers()
		}
	})

	var createAndWaitForRunning = func() {
		By("Create Redis: " + redis.Name)
		if redis.Spec.Mode != api.RedisModeCluster {

			redis.Spec.Mode = api.RedisModeStandalone
		}
		err = f.CreateRedis(redis)
		Expect(err).NotTo(HaveOccurred())

		By("Wait for Running redis")
		f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

		By("Wait for AppBinding to create")
		f.EventuallyAppBinding(redis.ObjectMeta).Should(BeTrue())

		By("Check valid AppBinding Specs")
		err := f.CheckAppBindingSpec(redis.ObjectMeta)
		Expect(err).NotTo(HaveOccurred())
	}

	var deleteTestResource = func() {
		if redis == nil {
			// No redis. So, no cleanup
			return
		}

		By("Check if Redis " + redis.Name + " exists.")
		rd, err := f.GetRedis(redis.ObjectMeta)
		if err != nil {
			if kerr.IsNotFound(err) {
				// Redis was not created. Hence, rest of cleanup is not necessary.
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}

		By("Update redis to set spec.terminationPolicy = WipeOut")
		_, err = f.PatchRedis(rd.ObjectMeta, func(in *api.Redis) *api.Redis {
			in.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
			return in
		})
		Expect(err).NotTo(HaveOccurred())

		By("Delete redis")
		err = f.DeleteRedis(redis.ObjectMeta)
		if err != nil {
			if kerr.IsNotFound(err) {
				// Redis was not created. Hence, rest of cleanup is not necessary.
				return
			}
			Expect(err).NotTo(HaveOccurred())
		}

		By("Wait for redis to be deleted")
		f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

		By("Wait for redis resources to be wipedOut")
		f.EventuallyWipedOut(redis.ObjectMeta).Should(Succeed())
	}

	AfterEach(func() {

		err = f.CleanupTestResources()
		Expect(err).NotTo(HaveOccurred())
		deleteTestResource()
	})

	var shouldSuccessfullyRunning = func() {
		if skipMessage != "" {
			Skip(skipMessage)
		}

		// Create Redis
		createAndWaitForRunning()
		res, err := f.TestConfig().GetPingResult(redis)
		Expect(err).NotTo(HaveOccurred())
		Expect(res).To(Equal("PONG"))

		By("Inserting item into database")
		f.EventuallySetItem(redis, key, value).Should(BeTrue())

		By("Retrieving item from database")
		f.EventuallyGetItem(redis, key).Should(BeEquivalentTo(value))
	}

	Describe("Test", func() {

		Context("General", func() {

			Context("-", func() {
				It("should run successfully", func() {

					shouldSuccessfullyRunning()

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for redis to be deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					// Create Redis object again to resume it
					By("Create Redis: " + redis.Name)
					err = f.CreateRedis(redis)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					By("Retrieving item from database")
					f.EventuallyGetItem(redis, key).Should(BeEquivalentTo(value))

				})
			})

			Context("PDB", func() {

				It("should run eviction successfully", func() {
					// Create Redis
					By("Create DB")
					createAndWaitForRunning()
					//Evict Redis pod
					By("Try to evict Pod")
					err := f.EvictPodsFromStatefulSet(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
				})

				It("should run eviction with shard successfully", func() {
					redis = f.RedisCluster()
					redis.Spec.Cluster = &api.RedisClusterSpec{
						Master:   types.Int32P(3),
						Replicas: types.Int32P(2),
					}
					// Create Redis
					By("Create DB")
					createAndWaitForRunning()

					// Wait for some more time until the 2nd thread of operator completes
					By(fmt.Sprintf("Wait for operator to complete processing the key %s/%s", redis.Namespace, redis.Name))
					time.Sleep(time.Minute * 3)

					//Evict a Redis pod from each sts and deploy
					By("Try to evict Pod")
					err := f.EvictPodsFromStatefulSet(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("Custom Resources", func() {

				Context("with custom SA Name", func() {
					BeforeEach(func() {
						redis.Spec.PodTemplate.Spec.ServiceAccountName = "my-custom-sa"
						redis.Spec.TerminationPolicy = api.TerminationPolicyHalt
					})

					It("should start and resume successfully", func() {
						//shouldTakeSnapshot()
						createAndWaitForRunning()
						By("Check if Redis " + redis.Name + " exists.")
						_, err := f.GetRedis(redis.ObjectMeta)
						if err != nil {
							if kerr.IsNotFound(err) {
								// Redis was not created. Hence, rest of cleanup is not necessary.
								return
							}
							Expect(err).NotTo(HaveOccurred())
						}

						By("Delete redis: " + redis.Name)
						err = f.DeleteRedis(redis.ObjectMeta)
						if err != nil {
							if kerr.IsNotFound(err) {
								// Redis was not created. Hence, rest of cleanup is not necessary.
								log.Infof("Skipping rest of cleanup. Reason: Redis %s is not found.", redis.Name)
								return
							}
							Expect(err).NotTo(HaveOccurred())
						}

						By("Wait for redis to be deleted")
						f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

						By("Resume DB")
						createAndWaitForRunning()
					})
				})

				Context("with custom SA", func() {
					var customSAForDB *core.ServiceAccount
					var customRoleForDB *rbac.Role
					var customRoleBindingForDB *rbac.RoleBinding
					BeforeEach(func() {
						customSAForDB = f.ServiceAccount()
						redis.Spec.PodTemplate.Spec.ServiceAccountName = customSAForDB.Name
						customRoleForDB = f.RoleForElasticsearch(redis.ObjectMeta)
						customRoleBindingForDB = f.RoleBinding(customSAForDB.Name, customRoleForDB.Name)
					})
					It("should take snapshot successfully", func() {
						By("Create Database SA")
						err = f.CreateServiceAccount(customSAForDB)
						Expect(err).NotTo(HaveOccurred())

						By("Create Database Role")
						err = f.CreateRole(customRoleForDB)
						Expect(err).NotTo(HaveOccurred())

						By("Create Database RoleBinding")
						err = f.CreateRoleBinding(customRoleBindingForDB)
						Expect(err).NotTo(HaveOccurred())

						createAndWaitForRunning()
					})
				})
			})
		})

		Context("Resume", func() {

			Context("Super Fast User - Create-Delete-Create-Delete-Create ", func() {
				It("should resume database successfully", func() {
					// Create and wait for running Redis
					createAndWaitForRunning()

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for redis to be deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					// Create Redis object again to resume it
					By("Create Redis: " + redis.Name)
					err = f.CreateRedis(redis)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					// Delete without caring if DB is resumed
					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Redis to be deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					// Create Redis object again to resume it
					By("Create Redis: " + redis.Name)
					err = f.CreateRedis(redis)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					_, err = f.GetRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("Basic Resume", func() {
				It("should resume database successfully", func() {

					shouldSuccessfullyRunning()

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for redis to be deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					// Create Redis object again to resume it
					By("Create Redis: " + redis.Name)
					err = f.CreateRedis(redis)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					By("Retrieving item from database")
					f.EventuallyGetItem(redis, key).Should(BeEquivalentTo(value))
				})
			})

			Context("Multiple times with PVC", func() {
				It("should resume database successfully", func() {

					shouldSuccessfullyRunning()

					for i := 0; i < 3; i++ {
						By(fmt.Sprintf("%v-th", i+1) + " time running.")
						By("Delete redis")
						err = f.DeleteRedis(redis.ObjectMeta)
						Expect(err).NotTo(HaveOccurred())

						By("Wait for redis to be deleted")
						f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

						// Create Redis object again to resume it
						By("Create Redis: " + redis.Name)
						err = f.CreateRedis(redis)
						Expect(err).NotTo(HaveOccurred())

						By("Wait for Running redis")
						f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

						By("Retrieving item from database")
						f.EventuallyGetItem(redis, key).Should(BeEquivalentTo(value))
					}
				})
			})
		})

		Context("Termination Policy", func() {

			Context("with TerminationPolicyDoNotTerminate", func() {
				BeforeEach(func() {
					redis.Spec.TerminationPolicy = api.TerminationPolicyDoNotTerminate
				})

				It("should work successfully", func() {
					// Create and wait for running Redis
					createAndWaitForRunning()

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).Should(HaveOccurred())

					By("Redis is not halted. Check for redis")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeTrue())

					By("Check for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					By("Update redis to set spec.terminationPolicy = Halt")
					_, err := f.PatchRedis(redis.ObjectMeta, func(in *api.Redis) *api.Redis {
						in.Spec.TerminationPolicy = api.TerminationPolicyHalt
						return in
					})
					Expect(err).NotTo(HaveOccurred())
				})
			})

			Context("with TerminationPolicyHalt", func() {
				var shouldRunWithTerminationHalt = func() {

					shouldSuccessfullyRunning()

					By("Halt Redis: Update redis to set spec.halted = true")
					_, err := f.PatchRedis(redis.ObjectMeta, func(in *api.Redis) *api.Redis {
						in.Spec.Halted = true
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Wait for halted redis")
					f.EventuallyRedisPhase(redis.ObjectMeta).Should(Equal(api.DatabasePhaseHalted))

					By("Resume Redis: Update redis to set spec.halted = false")
					_, err = f.PatchRedis(redis.ObjectMeta, func(in *api.Redis) *api.Redis {
						in.Spec.Halted = false
						return in
					})
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for redis to be deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					// Create Redis object again to resume it
					By("Create (halt) Redis: " + redis.Name)
					err = f.CreateRedis(redis)
					Expect(err).NotTo(HaveOccurred())

					By("Wait for Running redis")
					f.EventuallyRedisReady(redis.ObjectMeta).Should(BeTrue())

					By("Retrieving item from database")
					f.EventuallyGetItem(redis, key).Should(BeEquivalentTo(value))

				}

				It("should halt and resume successfully", shouldRunWithTerminationHalt)
			})

			Context("with TerminationPolicyDelete", func() {
				BeforeEach(func() {
					redis.Spec.TerminationPolicy = api.TerminationPolicyDelete
				})

				var shouldRunWithTerminationDelete = func() {

					shouldSuccessfullyRunning()

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("wait until redis is deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					By("Check for deleted PVCs")
					f.EventuallyPVCCount(redis.ObjectMeta).Should(Equal(0))
				}

				It("should run with TerminationPolicyDelete", shouldRunWithTerminationDelete)
			})

			Context("with TerminationPolicyWipeOut", func() {
				BeforeEach(func() {
					redis.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
				})

				var shouldRunWithTerminationWipeOut = func() {

					shouldSuccessfullyRunning()

					By("Delete redis")
					err = f.DeleteRedis(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					By("wait until redis is deleted")
					f.EventuallyRedis(redis.ObjectMeta).Should(BeFalse())

					By("Check for deleted PVCs")
					f.EventuallyPVCCount(redis.ObjectMeta).Should(Equal(0))
				}

				It("should run with TerminationPolicyDelete", shouldRunWithTerminationWipeOut)
			})
		})

		Context("Environment Variables", func() {
			var envList []core.EnvVar
			BeforeEach(func() {
				envList = []core.EnvVar{
					{
						Name:  "TEST_ENV",
						Value: "kubedb-redis-e2e",
					},
				}
				redis.Spec.PodTemplate.Spec.Env = envList
			})

			Context("Allowed Envs", func() {
				It("should run successfully with given Env", func() {
					createAndWaitForRunning()

					By("Checking pod started with given envs")
					pod, err := f.GetPod(redis.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())

					out, err := exec_util.ExecIntoPod(f.RestConfig(), pod, exec_util.Command("env"))
					Expect(err).NotTo(HaveOccurred())
					for _, env := range envList {
						Expect(out).Should(ContainSubstring(env.Name + "=" + env.Value))
					}

				})
			})

			Context("Update Envs", func() {
				It("should not reject to update Env", func() {
					createAndWaitForRunning()

					By("Updating Envs")
					_, _, err := util.PatchRedis(context.TODO(), f.ExtClient().KubedbV1alpha2(), redis, func(in *api.Redis) *api.Redis {
						in.Spec.PodTemplate.Spec.Env = []core.EnvVar{
							{
								Name:  "TEST_ENV",
								Value: "patched",
							},
						}
						return in
					}, metav1.PatchOptions{})

					Expect(err).NotTo(HaveOccurred())
				})
			})

		})

		Context("Custom config", func() {

			customConfigs := []string{
				"databases 10",
				"maxclients 500",
			}

			Context("from configMap", func() {
				var (
					userConfig *core.ConfigMap
				)

				BeforeEach(func() {
					userConfig = f.GetCustomConfig(customConfigs)
				})

				AfterEach(func() {
					By("Deleting configMap: " + userConfig.Name)
					err := f.DeleteConfigMap(userConfig.ObjectMeta)
					Expect(err).NotTo(HaveOccurred())
				})

				It("should set configuration provided in configMap", func() {
					if skipMessage != "" {
						Skip(skipMessage)
					}

					By("Creating configMap: " + userConfig.Name)
					err := f.CreateConfigMap(userConfig)
					Expect(err).NotTo(HaveOccurred())

					redis.Spec.ConfigSource = &core.VolumeSource{
						ConfigMap: &core.ConfigMapVolumeSource{
							LocalObjectReference: core.LocalObjectReference{
								Name: userConfig.Name,
							},
						},
					}

					// Create Redis
					createAndWaitForRunning()

					By("Checking redis configured from provided custom configuration")
					for _, cfg := range customConfigs {
						f.EventuallyRedisConfig(redis, cfg).Should(matcher.UseCustomConfig(cfg))
					}
				})
			})

		})

		Context("StorageType ", func() {

			var shouldRunSuccessfully = func() {

				if skipMessage != "" {
					Skip(skipMessage)
				}

				// Create Redis
				createAndWaitForRunning()

				By("Inserting item into database")
				f.EventuallySetItem(redis, key, value).Should(BeTrue())

				By("Retrieving item from database")
				f.EventuallyGetItem(redis, key).Should(BeEquivalentTo(value))
			}

			Context("Ephemeral", func() {

				BeforeEach(func() {
					redis.Spec.StorageType = api.StorageTypeEphemeral
					redis.Spec.Storage = nil
				})

				Context("General Behaviour", func() {

					BeforeEach(func() {
						redis.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
					})

					It("should run successfully", shouldRunSuccessfully)
				})

				Context("With TerminationPolicyHalt", func() {

					BeforeEach(func() {
						redis.Spec.TerminationPolicy = api.TerminationPolicyHalt
					})

					It("should reject to create Redis object", func() {

						By("Creating Redis: " + redis.Name)
						err := f.CreateRedis(redis)
						Expect(err).To(HaveOccurred())
					})
				})
			})
		})
	})
})
