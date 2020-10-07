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
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"time"

	api "kubedb.dev/apimachinery/apis/kubedb/v1alpha2"

	core "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"

	//"github.com/pkg/errors"
	"k8s.io/client-go/rest"
	"kmodules.xyz/client-go/tools/exec"
)

type TestConfig struct {
	RestConfig    *rest.Config
	DBCatalogName string
	KubeClient    kubernetes.Interface
	WithTLS       bool
}

// get output for "cluster nodes" command for specific node. For example : ( podName : redisName{i}-shard{j})
func (testConfig *TestConfig) GetClusterNodes(redis *api.Redis, i int, j int) (string, error) {
	var nodeConf string
	nodeConf = ""
	return nodeConf, wait.PollImmediate(time.Second*5, time.Minute, func() (bool, error) {
		command, err := testConfig.cmdClusterNodes()
		if err != nil {
			return false, err
		}
		pod, err := testConfig.GetDatabasePodForRedisCluster(redis, i, j)
		if err != nil {
			return false, err
		}
		nodeConf, err = exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
		if err != nil {
			return false, err
		}
		_, err = checkIfResultError(nodeConf)
		if err != nil {
			return false, err
		}

		return true, nil
	})
}

func (testConfig *TestConfig) GetPingResult(redis *api.Redis) (string, error) {
	var (
		pod *core.Pod
		err error
	)
	if redis.Spec.Mode == api.RedisModeStandalone {
		pod, err = testConfig.GetDatabasePodForRedisStandalone(redis)
	} else {
		pod, err = testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	}
	if err != nil {
		return "", err
	}
	command, err := testConfig.cmdPing(redis.Spec.Mode)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (testConfig *TestConfig) SetItem(redis *api.Redis, key string, value string) (string, error) {
	var (
		pod *core.Pod
		err error
	)
	if redis.Spec.Mode == api.RedisModeStandalone {
		pod, err = testConfig.GetDatabasePodForRedisStandalone(redis)
	} else {
		pod, err = testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	}
	if err != nil {
		return "", err
	}
	command, err := testConfig.cmdSetItem(redis.Spec.Mode, key, value)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}
func (testConfig *TestConfig) GetItem(redis *api.Redis, key string) (string, error) {
	var (
		pod *core.Pod
		err error
	)
	if redis.Spec.Mode == api.RedisModeStandalone {
		pod, err = testConfig.GetDatabasePodForRedisStandalone(redis)
	} else {
		pod, err = testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	}
	if err != nil {
		return "", err
	}

	command, err := testConfig.cmdGetItem(redis.Spec.Mode, key)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}

	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (testConfig *TestConfig) GetRedisConfig(redis *api.Redis, param string) ([]string, error) {
	var (
		pod *core.Pod
		err error
	)
	if redis.Spec.Mode == api.RedisModeStandalone {
		pod, err = testConfig.GetDatabasePodForRedisStandalone(redis)
	} else {
		pod, err = testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	}
	if err != nil {
		return nil, err
	}

	command, err := testConfig.cmdConfigGet(param)
	if err != nil {
		return nil, err
	}

	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return nil, err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return nil, err
	}
	out := strings.Split(res, "\n")
	return out, nil
}

func (testConfig *TestConfig) flushDB(podName string, nameSpace string) (string, error) {
	pod, err := testConfig.GetDatabasePodWithPodName(podName, nameSpace)
	if err != nil {
		return "", err
	}
	command, err := testConfig.cmdFlushDB()
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (testConfig *TestConfig) FlushDBForCluster(redis *api.Redis) (string, error) {
	var (
		ok    string
		err   error
		nodes [][]*RedisNode
	)
	err = wait.PollImmediate(time.Second*5, time.Minute*5, func() (bool, error) {
		nodes, err = testConfig.GetClusterNodesForCluster(redis)
		if err != nil {
			return false, err
		}
		time.Sleep(30 * time.Second)
		for i := 0; i < int(*redis.Spec.Cluster.Master); i++ {
			for j := 0; j < int(*redis.Spec.Cluster.Replicas+1); j++ {
				if nodes[i][j].Role == "slave" {
					continue
				}
				podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, i, j)
				ok, err = testConfig.flushDB(podName, redis.Namespace)
				if err != nil {
					return false, err
				}
				ok, err = checkIfResultError(ok)
				if err != nil {
					return false, err
				}

			}
		}
		return true, nil
	})
	return ok, err
}
func (testConfig *TestConfig) FlushDBForStandalone(redis *api.Redis) (string, error) {
	var (
		ok  string
		err error
	)

	podName := fmt.Sprintf("%s-%d", redis.Name, 0)
	ok, err = testConfig.flushDB(podName, redis.Namespace)
	if err != nil {
		return "", err
	}
	ok, err = checkIfResultError(ok)
	if err != nil {
		return "", err
	}

	return ok, nil
}
func (testConfig *TestConfig) GetDBSizeForStandalone(redis *api.Redis) (string, error) {

	pod, err := testConfig.GetDatabasePodForRedisStandalone(redis)
	if err != nil {
		return "", err
	}
	command, err := testConfig.cmdGetDBSize()
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}

	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (testConfig *TestConfig) GetDbSizeForCluster(redis *api.Redis) (int64, error) {
	var totalSize int64
	totalSize = 0

	for i := 0; i < int(*redis.Spec.Cluster.Master); i++ {
		for j := 0; j < int(*redis.Spec.Cluster.Replicas+1); j++ {
			podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, i, j)
			pod, err := testConfig.GetDatabasePodWithPodName(podName, redis.Namespace)
			if err != nil {
				return -1, err
			}
			command, err := testConfig.cmdGetDBSize()
			if err != nil {
				return -1, err
			}
			res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
			if err != nil {
				return -1, err
			}
			res, err = checkIfResultError(res)
			if err != nil {
				return -1, err
			}
			out, err := strconv.ParseInt(res, 10, 64)
			if err != nil {
				return -1, err
			}

			totalSize += out

		}
	}
	return totalSize, nil
}

func (testConfig *TestConfig) GetDbSizeForIndividualNodeInCluster(redis *api.Redis, i int, j int) (int64, error) {

	pod, err := testConfig.GetDatabasePodForRedisCluster(redis, i, j)
	if err != nil {
		return -1, err
	}
	command, err := testConfig.cmdGetDBSize()
	if err != nil {
		return -1, err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return -1, err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return -1, err
	}

	out, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		return -1, err
	}

	return out, nil
}

func (testConfig *TestConfig) ClusterFailOver(redis *api.Redis, i int, j int) (string, error) {

	podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, i, j)
	pod, err := testConfig.GetDatabasePodWithPodName(podName, redis.Namespace)
	if err != nil {
		return "", err
	}
	command, err := testConfig.cmdClusterFailOver()
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}

	return res, nil
}

func (testConfig *TestConfig) GetClusterSlots(redis *api.Redis) ([]ClusterSlot, error) {
	podName := fmt.Sprintf("%s-shard%d-%d", redis.Name, 0, 0)
	pod, err := testConfig.GetDatabasePodWithPodName(podName, redis.Namespace)
	if err != nil {
		return nil, err
	}
	command, err := testConfig.cmdClusterSlots()
	if err != nil {
		return nil, err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return nil, err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return nil, err
	}
	out, err := convertIntoClusterSlots(res)
	if err != nil {
		return nil, err
	}

	return out, nil
}

func (testConfig *TestConfig) DeleteItem(redis *api.Redis, key string) (string, error) {
	var (
		pod *core.Pod
		err error
	)
	if redis.Spec.Mode == api.RedisModeStandalone {
		pod, err = testConfig.GetDatabasePodForRedisStandalone(redis)
	} else {
		pod, err = testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	}
	if err != nil {
		return "", err
	}

	command, err := testConfig.cmdDeleteItem(redis.Spec.Mode, key)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (testConfig *TestConfig) GetRandomKey(redis *api.Redis) (string, error) {
	var (
		pod *core.Pod
		err error
	)
	if redis.Spec.Mode == api.RedisModeStandalone {
		pod, err = testConfig.GetDatabasePodForRedisStandalone(redis)
	} else {
		randValue := rand.Int()
		randValue = randValue % int(*redis.Spec.Cluster.Master)
		pod, err = testConfig.GetDatabasePodForRedisCluster(redis, randValue, 0)
	}
	if err != nil {
		return "", err
	}

	command, err := testConfig.cmdRandomKey(redis.Spec.Mode)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil

}

func (testConfig *TestConfig) GetClusterNodesForCluster(redis *api.Redis) ([][]*RedisNode, error) {
	var (
		nodes      = make([][]*RedisNode, int(*redis.Spec.Cluster.Master))
		start, end int
		nodesConf  string
		slotRange  []string
		err        error
	)

	for i := 0; i < int(*redis.Spec.Cluster.Master); i++ {
		nodes[i] = make([]*RedisNode, int(*redis.Spec.Cluster.Replicas)+1)

		for j := 0; j <= int(*redis.Spec.Cluster.Replicas); j++ {

			nodesConf, err = testConfig.GetClusterNodes(redis, i, j)
			if err != nil {
				return nil, err
			}
			nodesConf, err = checkIfResultError(nodesConf)
			if err != nil {
				return nodes, err
			}

			nodesConf = strings.TrimSpace(nodesConf)
			for _, info := range strings.Split(nodesConf, "\n") {
				info = strings.TrimSpace(info)

				if strings.Contains(info, "myself") {
					parts := strings.Split(info, " ")

					node := &RedisNode{
						ID: parts[0],
						IP: strings.Split(parts[1], ":")[0],
					}

					if strings.Contains(parts[2], "slave") {
						node.Role = "slave"
						node.MasterID = parts[3]
					} else {
						node.Role = "master"
						node.SlotsCnt = 0

						for k := 8; k < len(parts); k++ {
							if parts[k][0] == '[' && parts[k][len(parts[k])-1] == ']' {
								continue
							}

							slotRange = strings.Split(parts[k], "-")

							// slotRange contains only int. So errors are ignored
							start, _ = strconv.Atoi(slotRange[0])
							if len(slotRange) == 1 {
								end = start
							} else {
								end, _ = strconv.Atoi(slotRange[1])
							}

							node.SlotStart = append(node.SlotStart, start)
							node.SlotEnd = append(node.SlotEnd, end)
							node.SlotsCnt += (end - start) + 1
						}
					}
					nodes[i][j] = node
					break
				}
			}
		}
	}

	return nodes, nil
}

func (testConfig *TestConfig) GetClusterInfoForRedis(redis *api.Redis) (string, error) {

	command, err := testConfig.cmdClusterInfo()
	if err != nil {
		return "false", err
	}
	pod, err := testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}

	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}
func (testConfig *TestConfig) GetClusterSaveConfig(redis *api.Redis) (string, error) {
	command, err := testConfig.cmdClusterSaveConfig()
	if err != nil {
		return "", err
	}
	pod, err := testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	if err != nil {
		return "", err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return "", err
	}

	res, err = checkIfResultError(res)
	if err != nil {
		return "", err
	}
	return res, nil
}

func (testConfig *TestConfig) GetClusterCountKeysInSlot(redis *api.Redis, slot int) (int64, error) {
	command, err := testConfig.cmdClusterCountKeysInSlot(slot)
	if err != nil {
		return -1, err
	}
	pod, err := testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	if err != nil {
		return -1, err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return -1, err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return -1, err
	}

	out, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		return -1, err
	}

	return out, nil
}

func (testConfig *TestConfig) GetClusterCountFailureReports(redis *api.Redis, nodeID string) (int64, error) {
	command, err := testConfig.cmdClusterCountFailureReports(nodeID)
	if err != nil {
		return -1, err
	}
	pod, err := testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	if err != nil {
		return -1, err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return -1, err
	}
	res, err = checkIfResultError(res)
	if err != nil {
		return -1, err
	}

	out, err := strconv.ParseInt(res, 10, 64)
	if err != nil {
		return -1, err
	}

	return out, nil
}

func (testConfig *TestConfig) GetClusterSlaves(redis *api.Redis, nodeID string) ([]string, error) {
	command, err := testConfig.cmdClusterSlaves(nodeID)
	if err != nil {
		return nil, err
	}
	pod, err := testConfig.GetDatabasePodForRedisCluster(redis, 0, 0)
	if err != nil {
		return nil, err
	}
	res, err := exec.ExecIntoPod(testConfig.RestConfig, pod, exec.Command(command...))
	if err != nil {
		return nil, err
	}

	res, err = checkIfResultError(res)
	if err != nil {
		return nil, err
	}
	out := strings.Split(res, "\n")

	return out, nil
}
