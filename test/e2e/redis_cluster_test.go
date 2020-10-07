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
	"fmt"
	"strconv"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha1"
	test_util "kubedb.dev/redis/pkg/testing"

	// test_util "kubedb.dev/redis/pkg/testing"
	"kubedb.dev/redis/test/e2e/framework"

	"github.com/appscode/go/sets"
	"github.com/appscode/go/types"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kerr "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
)

var createAndWaitForRunning = func() {
	By("Create Redis: " + cl.redis.Name)
	err := cl.f.CreateRedis(cl.redis)
	Expect(err).NotTo(HaveOccurred())

	By("Wait for Running redis")
	cl.f.EventuallyRedisReady(cl.redis.ObjectMeta).Should(BeTrue())
}

//
var deleteTestResource = func() {
	By("Check if Redis " + cl.redis.Name + " exists.")
	rd, err := cl.f.GetRedis(cl.redis.ObjectMeta)
	if err != nil {
		if kerr.IsNotFound(err) {
			// Redis was not created. Hence, rest of cleanup is not necessary.
			return
		}
		Expect(err).NotTo(HaveOccurred())
	}

	By("Update redis to set spec.terminationPolicy = WipeOut")
	_, err = cl.f.PatchRedis(rd.ObjectMeta, func(in *api.Redis) *api.Redis {
		in.Spec.TerminationPolicy = api.TerminationPolicyWipeOut
		return in
	})
	Expect(err).NotTo(HaveOccurred())

	By("Delete redis")
	err = cl.f.DeleteRedis(cl.redis.ObjectMeta)
	if err != nil {
		if kerr.IsNotFound(err) {
			// Redis was not created. Hence, rest of cleanup is not necessary.
			return
		}
		Expect(err).NotTo(HaveOccurred())
	}

	By("Wait for redis to be deleted")
	cl.f.EventuallyRedis(cl.redis.ObjectMeta).Should(BeFalse())

	By("Wait for redis resources to be wipedOut cluster")
	cl.f.EventuallyWipedOut(cl.redis.ObjectMeta).Should(Succeed())
}

var _ = Describe("Redis Cluster", func() {

	BeforeEach(func() {
		if !framework.Cluster {
			Skip("cluster test is disabled")

		}
	})

	var (
		err                  error
		skipMessage          string
		failover             bool
		cluster              *test_util.ClusterNodes
		nodes                [][]*test_util.RedisNode
		expectedClusterSlots []test_util.ClusterSlot
	)

	var waitUntilConfiguredRedisCluster = func() error {
		var (
			err   error
			slots []test_util.ClusterSlot
		)

		err = wait.PollImmediate(time.Second*5, time.Minute*2, func() (bool, error) {
			slots, err = cl.f.TestConfig().GetClusterSlots(cl.redis)
			if err != nil {
				return false, nil
			}

			total := 0
			masterIds := sets.NewString()
			checkReplicas := true
			for _, slot := range slots {
				total += slot.End - slot.Start + 1
				masterIds.Insert(slot.Nodes[0].Id)
				checkReplicas = checkReplicas && (len(slot.Nodes)-1 == int(*cl.redis.Spec.Cluster.Replicas))
			}

			if total != 16384 || masterIds.Len() != int(*cl.redis.Spec.Cluster.Master) || !checkReplicas {

				return false, nil
			}

			return true, nil

		})
		return err

	}

	var getConfiguredClusterInfo = func() {
		skipMessage = ""
		if !framework.Cluster {
			skipMessage = "cluster test is disabled"
		}

		By("Wait until redis cluster be configured")
		Expect(waitUntilConfiguredRedisCluster()).NotTo(HaveOccurred())

		By("Get configured cluster info")
		nodes, err = cl.f.TestConfig().GetClusterNodesForCluster(cl.redis)
		Expect(err).NotTo(HaveOccurred())

		cluster = &test_util.ClusterNodes{
			Nodes: nodes,
		}

	}

	var assertSimple = func() {
		It("should GET/SET/DEL", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			if !failover {
				res, err := cl.f.TestConfig().GetItem(cl.redis, "A")
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal(""))
				_, err = cl.f.TestConfig().SetItem(cl.redis, "A", "VALUE")
				Expect(err).NotTo(HaveOccurred())
			}

			res, err := cl.f.TestConfig().GetItem(cl.redis, "A")
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("VALUE"))
		})
	}

	Context("Cluster Commands", func() {
		BeforeEach(func() {
			getConfiguredClusterInfo()
		})

		AfterEach(func() {

			if framework.Cluster {
				_, err := cl.f.TestConfig().FlushDBForCluster(cl.redis)
				Expect(err).NotTo(HaveOccurred())
				err = cl.f.CleanupTestResources()
				Expect(err).NotTo(HaveOccurred())
			}

		})

		It("should CLUSTER INFO", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			res, err := cl.f.TestConfig().GetClusterInfoForRedis(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(ContainSubstring(fmt.Sprintf("cluster_known_nodes:%d",
				(*cl.redis.Spec.Cluster.Master)*((*cl.redis.Spec.Cluster.Replicas)+1))))

			for i := 0; i < 10; i++ {
				_, err := cl.f.TestConfig().SetItem(cl.redis, fmt.Sprintf("%d", i), "")
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("calls fn for every master node", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			for i := 0; i < 10; i++ {
				res, err := cl.f.TestConfig().SetItem(cl.redis, strconv.Itoa(i), "")
				Expect(err).NotTo(HaveOccurred())
				Expect(res).To(Equal("OK"))
			}

			_, err := cl.f.TestConfig().FlushDBForCluster(cl.redis)
			Expect(err).NotTo(HaveOccurred())

			size, err := cl.f.TestConfig().GetDbSizeForCluster(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(size).To(Equal(int64(0)))
		})

		It("should CLUSTER SLOTS", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			slots, err := cl.f.TestConfig().GetClusterSlots(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(slots).To(HaveLen(3))

			wanted := []test_util.ClusterSlot{
				{
					Start: 0,
					End:   5460,
					Nodes: cluster.ClusterNodes(0, 5460),
				}, {
					Start: 5461,
					End:   10922,
					Nodes: cluster.ClusterNodes(5461, 10922),
				}, {
					Start: 10923,
					End:   16383,
					Nodes: cluster.ClusterNodes(10923, 16383),
				},
			}

			Expect(cl.f.TestConfig().AssertSlotsEqual(slots, wanted)).NotTo(HaveOccurred())
		})

		It("should CLUSTER NODES", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			res, err := cl.f.TestConfig().GetClusterNodes(cl.redis, 0, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(len(res)).To(BeNumerically(">", 400))
		})

		It("should CLUSTER COUNT-FAILURE-REPORTS", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			n, err := cl.f.TestConfig().GetClusterCountFailureReports(cl.redis, cluster.Nodes[0][0].ID)
			Expect(err).NotTo(HaveOccurred())
			Expect(n).To(Equal(int64(0)))
		})

		It("should CLUSTER COUNTKEYSINSLOT", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			res, err := cl.f.TestConfig().GetClusterCountKeysInSlot(cl.redis, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal(int64(0)))
		})

		It("should CLUSTER SAVECONFIG", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			res, err := cl.f.TestConfig().GetClusterSaveConfig(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(Equal("OK"))
		})

		It("should CLUSTER SLAVES", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			for i := 0; i < len(nodes); i++ {
				if nodes[i][0].Role == "master" {
					slaveList, err := cl.f.TestConfig().GetClusterSlaves(cl.redis, cluster.Nodes[i][0].ID)
					Expect(err).NotTo(HaveOccurred())
					Expect(slaveList).Should(ContainElement(ContainSubstring("slave")))
					Expect(slaveList).Should(HaveLen(1))
					break
				}
			}
		})

		It("should RANDOMKEY", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			const nkeys = 100

			for i := 0; i < nkeys; i++ {
				_, err := cl.f.TestConfig().SetItem(cl.redis, fmt.Sprintf("key%d", i), "value")
				Expect(err).NotTo(HaveOccurred())
			}

			var keys []string
			addKey := func(key string) {
				for _, k := range keys {
					if k == key {
						return
					}
				}
				keys = append(keys, key)
			}

			for i := 0; i < nkeys*10; i++ {
				key, err := cl.f.TestConfig().GetRandomKey(cl.redis)
				Expect(err).NotTo(HaveOccurred())
				addKey(key)
			}

			Expect(len(keys)).To(BeNumerically("~", nkeys, nkeys/10))
		})

		assertSimple()
		//assertPubSub()
	})

	Context("Cluster failover", func() {
		JustBeforeEach(func() {
			failover = true

			getConfiguredClusterInfo()

			for i := 0; i < int(*cl.redis.Spec.Cluster.Master); i++ {
				for j := 0; j <= int(*cl.redis.Spec.Cluster.Replicas); j++ {
					if nodes[i][j].Role == "slave" {
						res, err := cl.f.TestConfig().GetDbSizeForIndividualNodeInCluster(cl.redis, i, j)
						Expect(err).NotTo(HaveOccurred())
						Expect(res).Should(Equal(int64(0)))

					}
				}
			}

			_, err = cl.f.TestConfig().SetItem(cl.redis, "A", "VALUE")
			Expect(err).NotTo(HaveOccurred())

			slots, err := cl.f.TestConfig().GetClusterSlots(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			totalSlots := test_util.AvailableClusterSlots(slots)
			Expect(totalSlots).To(Equal(16384))

			for i := 0; i < int(*cl.redis.Spec.Cluster.Master); i++ {
				for j := 0; j <= int(*cl.redis.Spec.Cluster.Replicas); j++ {
					if nodes[i][j].Role == "slave" {
						res, err := cl.f.TestConfig().ClusterFailOver(cl.redis, i, j)
						Expect(err).NotTo(HaveOccurred())
						Expect(res).To(Equal("OK"))
						time.Sleep(time.Second * 7)

						slots, err := cl.f.TestConfig().GetClusterSlots(cl.redis)
						Expect(err).NotTo(HaveOccurred())

						totalSlots := test_util.AvailableClusterSlots(slots)
						Expect(totalSlots).To(Equal(16384))

						break
					}
				}
			}

		})

		AfterEach(func() {
			failover = false

			err = cl.f.CleanupTestResources()
			Expect(err).NotTo(HaveOccurred())

		})

		assertSimple()
	})

	Context("Modify cluster", func() {
		It("should configure according to modified redis crd", func() {
			if skipMessage != "" {
				Skip(skipMessage)
			}

			By("Add replica")
			cl.redis, err = cl.f.PatchRedis(cl.redis.ObjectMeta, func(in *api.Redis) *api.Redis {
				in.Spec.Cluster.Replicas = types.Int32P((*cl.redis.Spec.Cluster.Replicas) + 1)
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Wait until statefulsets are ready")
			Expect(cl.f.WaitUntilStatefulSetReady(cl.redis)).NotTo(HaveOccurred())

			getConfiguredClusterInfo()
			time.Sleep(time.Minute)
			cluster.Nodes, err = cl.f.TestConfig().GetClusterNodesForCluster(cl.redis)
			Expect(err).NotTo(HaveOccurred())

			By("cluster slots should be configured as expected")
			expectedClusterSlots = []test_util.ClusterSlot{
				{
					Start: 0,
					End:   5460,
					Nodes: cluster.ClusterNodes(0, 5460),
				}, {
					Start: 5461,
					End:   10922,
					Nodes: cluster.ClusterNodes(5461, 10922),
				}, {
					Start: 10923,
					End:   16383,
					Nodes: cluster.ClusterNodes(10923, 16383),
				},
			}

			res, err := cl.f.TestConfig().GetClusterSlots(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))
			err = cl.f.TestConfig().AssertSlotsEqual(res, expectedClusterSlots)

			Expect(err).ShouldNot(HaveOccurred())

			// =======================================

			By("Remove replica")
			cl.redis, err = cl.f.PatchRedis(cl.redis.ObjectMeta, func(in *api.Redis) *api.Redis {
				in.Spec.Cluster.Replicas = types.Int32P((*cl.redis.Spec.Cluster.Replicas) - 1)
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Wait until statefulsets are ready")
			Expect(cl.f.WaitUntilStatefulSetReady(cl.redis)).NotTo(HaveOccurred())
			getConfiguredClusterInfo()

			time.Sleep(time.Minute)
			cluster.Nodes, err = cl.f.TestConfig().GetClusterNodesForCluster(cl.redis)
			Expect(err).NotTo(HaveOccurred())

			By("cluster slots should be configured as expected")
			expectedClusterSlots = []test_util.ClusterSlot{
				{
					Start: 0,
					End:   5460,
					Nodes: cluster.ClusterNodes(0, 5460),
				}, {
					Start: 5461,
					End:   10922,
					Nodes: cluster.ClusterNodes(5461, 10922),
				}, {
					Start: 10923,
					End:   16383,
					Nodes: cluster.ClusterNodes(10923, 16383),
				},
			}
			res, err = cl.f.TestConfig().GetClusterSlots(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(3))
			err = cl.f.TestConfig().AssertSlotsEqual(res, expectedClusterSlots)
			Expect(err).ShouldNot(HaveOccurred())

			// =======================================

			By("Add master")
			cl.redis, err = cl.f.PatchRedis(cl.redis.ObjectMeta, func(in *api.Redis) *api.Redis {
				in.Spec.Cluster.Master = types.Int32P((*cl.redis.Spec.Cluster.Master) + 1)
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Wait until statefulsets are ready")
			Expect(cl.f.WaitUntilStatefulSetReady(cl.redis)).NotTo(HaveOccurred())
			getConfiguredClusterInfo()

			time.Sleep(time.Minute)
			cluster.Nodes, err = cl.f.TestConfig().GetClusterNodesForCluster(cl.redis)
			Expect(err).NotTo(HaveOccurred())

			By("cluster slots should be configured as expected")
			expectedClusterSlots = []test_util.ClusterSlot{
				{
					Start: 1365,
					End:   5460,
					Nodes: cluster.ClusterNodes(1365, 5460),
				}, {
					Start: 6827,
					End:   10922,
					Nodes: cluster.ClusterNodes(6827, 10922),
				}, {
					Start: 12288,
					End:   16383,
					Nodes: cluster.ClusterNodes(12288, 16383),
				}, {
					Start: 0,
					End:   1364,
					Nodes: cluster.ClusterNodes(0, 1364),
				}, {
					Start: 5461,
					End:   6826,
					Nodes: cluster.ClusterNodes(5461, 6826),
				}, {
					Start: 10923,
					End:   12287,
					Nodes: cluster.ClusterNodes(10923, 12287),
				},
			}
			res, err = cl.f.TestConfig().GetClusterSlots(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(6))
			err = cl.f.TestConfig().AssertSlotsEqual(res, expectedClusterSlots)
			Expect(err).ShouldNot(HaveOccurred())

			// =======================================

			By("Remove master")
			cl.redis, err = cl.f.PatchRedis(cl.redis.ObjectMeta, func(in *api.Redis) *api.Redis {
				in.Spec.Cluster.Master = types.Int32P((*cl.redis.Spec.Cluster.Master) - 1)
				return in
			})
			Expect(err).NotTo(HaveOccurred())

			By("Wait until statefulsets are ready")
			Expect(cl.f.WaitUntilStatefulSetReady(cl.redis)).NotTo(HaveOccurred())

			getConfiguredClusterInfo()
			time.Sleep(time.Minute)
			cluster.Nodes, err = cl.f.TestConfig().GetClusterNodesForCluster(cl.redis)
			Expect(err).NotTo(HaveOccurred())

			By("cluster slots should be configured as expected")
			expectedClusterSlots = []test_util.ClusterSlot{
				{
					Start: 0,
					End:   5460,
					Nodes: cluster.ClusterNodes(0, 5460),
				}, {
					Start: 5461,
					End:   6825,
					Nodes: cluster.ClusterNodes(5461, 6825),
				}, {
					Start: 6827,
					End:   10922,
					Nodes: cluster.ClusterNodes(6827, 10922),
				}, {
					Start: 6826,
					End:   6826,
					Nodes: cluster.ClusterNodes(6826, 6826),
				}, {
					Start: 10923,
					End:   16383,
					Nodes: cluster.ClusterNodes(10923, 16383),
				},
			}
			res, err = cl.f.TestConfig().GetClusterSlots(cl.redis)
			Expect(err).NotTo(HaveOccurred())
			Expect(res).To(HaveLen(5))
			err = cl.f.TestConfig().AssertSlotsEqual(res, expectedClusterSlots)
			Expect(err).ShouldNot(HaveOccurred())

		})
	})
})
