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

package testing

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	core "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (testConfig *TestConfig) GetDatabasePodWithPodName(podName string, podNamespace string) (*core.Pod, error) {
	return testConfig.KubeClient.CoreV1().Pods(podNamespace).Get(context.TODO(), podName, metav1.GetOptions{})
}
func (testConfig *TestConfig) GetDatabasePodForRedisStandalone(redis *api.Redis) (*core.Pod, error) {
	podName := fmt.Sprintf("%s-%d", redis.Name, 0)

	return testConfig.KubeClient.CoreV1().Pods(redis.Namespace).Get(context.TODO(), podName, metav1.GetOptions{})
}
func (testConfig *TestConfig) GetDatabasePodForRedisCluster(redis *api.Redis, i int, j int) (*core.Pod, error) {
	podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, i, j)

	return testConfig.KubeClient.CoreV1().Pods(redis.Namespace).Get(context.TODO(), podName, metav1.GetOptions{})
}

type RedisNode struct {
	SlotStart []int
	SlotEnd   []int
	SlotsCnt  int

	ID       string
	IP       string
	Port     string
	Role     string
	Down     bool
	MasterID string

	Master *RedisNode
	Slaves []*RedisNode
}
type ClusterNode struct {
	Id   string
	Addr string
}

type ClusterNodes struct {
	Nodes [][]*RedisNode
}
type ClusterSlot struct {
	Start int
	End   int
	Nodes []ClusterNode
}

func slotEqual(s1, s2 ClusterSlot) bool {
	if s1.Start != s2.Start {
		return false
	}
	if s1.End != s2.End {
		return false
	}
	if len(s1.Nodes) != len(s2.Nodes) {
		return false
	}
	for i, n1 := range s1.Nodes {
		if n1.Addr != s2.Nodes[i].Addr {
			return false
		}
	}
	return true
}

func (testConfig *TestConfig) AssertSlotsEqual(slots, wanted []ClusterSlot) error {
	for _, s2 := range wanted {
		ok := false
		for _, s1 := range slots {

			if slotEqual(s1, s2) {
				ok = true
				break
			}
		}
		if ok {
			continue
		}
		return fmt.Errorf("%v not found in %v", s2, slots)
	}
	return nil
}

func (s *ClusterNodes) ClusterNodes(slotStart, slotEnd int) []ClusterNode {
	for i := 0; i < len(s.Nodes); i++ {
		for k := 0; k < len(s.Nodes[i][0].SlotStart); k++ {
			if s.Nodes[i][0].SlotStart[k] == slotStart && s.Nodes[i][0].SlotEnd[k] == slotEnd {
				nodes := make([]ClusterNode, len(s.Nodes[i]))
				for j := 0; j < len(s.Nodes[i]); j++ {
					nodes[j] = ClusterNode{
						Id:   "",
						Addr: net.JoinHostPort(s.Nodes[i][j].IP, "6379"),
					}
				}

				return nodes
			}
		}
	}

	return nil
}

func convertIntoClusterSlots(result string) ([]ClusterSlot, error) {

	var (
		slots []ClusterSlot
		err   error
		ip    string
		port  string
		id    string
		add   string
		start int
		end   int
	)
	result = strings.TrimSpace(result)
	strSlice := strings.Split(result, "\n")

	for i := 0; i < len(strSlice); {
		var slot ClusterSlot
		start, err = strconv.Atoi(strSlice[i])
		i++
		if err != nil {
			return nil, err
		}
		end, err = strconv.Atoi(strSlice[i])
		i++
		if err != nil {
			return nil, err
		}
		var nodes []ClusterNode

		for i < len(strSlice) && checkIPAddress(strSlice[i]) {
			ip = strSlice[i]
			i++
			port = strSlice[i]
			i++
			id = strSlice[i]
			i++
			add = net.JoinHostPort(ip, port)
			node := ClusterNode{
				Id:   id,
				Addr: add,
			}
			nodes = append(nodes, node)
		}
		slot = ClusterSlot{
			Start: start,
			End:   end,
			Nodes: nodes,
		}
		slots = append(slots, slot)

	}

	return slots, nil
}

func AvailableClusterSlots(slots []ClusterSlot) int {

	total := 0
	for i := 0; i < len(slots); i++ {
		total += slots[i].End - slots[i].Start + 1
	}
	return total
}

func checkIPAddress(ip string) bool {
	return net.ParseIP(ip) != nil
}
