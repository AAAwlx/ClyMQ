package zookeeper

import (
	"ClyMQ/logger"
	"encoding/json"

	// "fmt"
	"reflect"
	"time"

	"github.com/go-zookeeper/zk"
)

type ZK struct {
	conn *zk.Conn

	Root       string
	BrokerRoot string
	TopicRoot  string
}

type ZkInfo struct {
	HostPorts []string
	Timeout   int
	Root      string
}

//root = "/ClyMQ"
func NewZK(info ZkInfo) *ZK {
	coon, _, err := zk.Connect(info.HostPorts, time.Duration(info.Timeout)*time.Second)
	if err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}
	return &ZK{
		conn:       coon,
		Root:       info.Root,
		BrokerRoot: info.Root + "/Brokers",
		TopicRoot:  info.Root + "/Topics",
	}
}

type BrokerNode struct {
	Name         string `json:"name"`
	BrokHostPort string `json:"brokhostport"`
	RaftHostPort string `json:"rafthostport"`
	Me 			 int 	`json:"me"`
	Pnum         int    `json:"pNum"`
	//一些负载情况
}

type TopicNode struct {
	Name string `json:"name"`
	Pnum int    `json:"pNum"`
	// Brokers []string `json:"brokers"` //保存该topic的partition现在有那些broker负责，
	//用于PTP的情况
}

type PartitionNode struct {
	Name      string `json:"name"`
	TopicName string `json:"topicName"`
	Index     int64  `json:"index"`
	Option    int8   `json:"option"` //partition的状态
	DupNum    int8   `json:"dupNum"`
	PTPoffset int64  `json:"ptpOffset"`
}

type SubscriptionNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topic"`
	PartitionName string `json:"part"`
	Option        int8   `json:"option"`
	Groups        []byte `json:"groups"`
}

type BlockNode struct {
	Name          string `json:"name"`
	FileName      string `json:"filename"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`

	LeaderBroker string `json:"leaderBroker"`
}

type DuplicateNode struct {
	Name          string `json:"name"`
	TopicName     string `json:"topicName"`
	PartitionName string `json:"partitionName"`
	BlockName     string `json:"blockname"`
	StartOffset   int64  `json:"startOffset"`
	EndOffset     int64  `json:"endOffset"`
	BrokerName    string `json:"brokerName"`
}

type Map struct {
	Consumers map[string]bool `json:"consumer"`
}

// RegisterNode 将指定的节点信息注册到 ZooKeeper。
// 参数:
//   - znode: 要注册的节点对象，类型可以是 BrokerNode、TopicNode、PartitionNode、BlockNode、DuplicateNode 或 SubscriptionNode。
// 返回值:
//   - err: 如果在处理或注册节点时发生错误，则返回相应的错误信息。
func (z *ZK) RegisterNode(znode interface{}) (err error) {
	path := ""            // 用于存储节点路径
	var data []byte      // 存储节点数据的字节切片
	var bnode BrokerNode
	var tnode TopicNode
	var pnode PartitionNode
	var blnode BlockNode
	var dnode DuplicateNode
	var snode SubscriptionNode

	// 获取 znode 的类型
	i := reflect.TypeOf(znode)
	switch i.Name() {
	case "BrokerNode":
		bnode = znode.(BrokerNode)
		path += z.BrokerRoot + "/" + bnode.Name
		data, err = json.Marshal(bnode) // 将节点数据序列化为 JSON
	case "TopicNode":
		tnode = znode.(TopicNode)
		path += z.TopicRoot + "/" + tnode.Name
		data, err = json.Marshal(tnode) // 将节点数据序列化为 JSON
	case "PartitionNode":
		pnode = znode.(PartitionNode)
		path += z.TopicRoot + "/" + pnode.TopicName + "/Partitions/" + pnode.Name
		data, err = json.Marshal(pnode) // 将节点数据序列化为 JSON
	case "SubscriptionNode":
		snode = znode.(SubscriptionNode)
		path += z.TopicRoot + "/" + snode.TopicName + "/Subscriptions/" + snode.Name
	case "BlockNode":
		blnode = znode.(BlockNode)
		path += z.TopicRoot + "/" + blnode.TopicName + "/Partitions/" + blnode.PartitionName + "/" + blnode.Name
		data, err = json.Marshal(blnode) // 将节点数据序列化为 JSON
	case "DuplicateNode":
		dnode = znode.(DuplicateNode)
		path += z.TopicRoot + "/" + dnode.TopicName + "/Partitions/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.BrokerName
		data, err = json.Marshal(dnode) // 将节点数据序列化为 JSON
	}

	// 如果序列化数据时发生错误，记录错误日志并返回错误
	if err != nil {
		logger.DEBUG(logger.DError, "the node %v turn json fail%v\n", path, err.Error())
		return err
	}

	// 检查节点路径是否已经存在
	ok, _, _ := z.conn.Exists(path)
	if ok {
		// 节点已经存在，更新节点的数据
		logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", path)
		_, state, _ := z.conn.Get(path)
		_, err = z.conn.Set(path, data, state.Version) // 更新节点的数据
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v Set fail %v\n", path, err.Error())
			return err
		}
	} else {
		// 节点不存在，创建新的节点
		_, err = z.conn.Create(path, data, 0, zk.WorldACL(zk.PermAll))
		if err != nil {
			logger.DEBUG(logger.DError, "the node %v Create fail %v\n", path, err.Error())
			return err
		}
	}

	// 如果节点类型是 TopicNode，创建 Partitions 和 Subscriptions 子节点
	if i.Name() == "TopicNode" {
		// 创建 Partitions 节点
		partitions_path := path + "/Partitions"
		ok, _, err = z.conn.Exists(partitions_path)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", partitions_path)
		} else {
			_, err = z.conn.Create(partitions_path, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				logger.DEBUG(logger.DError, "the node %v Create fail\n", partitions_path)
				return err
			}
		}

		// 创建 Subscriptions 节点
		subscription_path := path + "/Subscriptions"
		ok, _, err = z.conn.Exists(subscription_path)
		if ok {
			logger.DEBUG(logger.DLog, "the node %v had in zookeeper\n", subscription_path)
		} else {
			_, err = z.conn.Create(subscription_path, nil, 0, zk.WorldACL(zk.PermAll))
			if err != nil {
				logger.DEBUG(logger.DError, "the node %v Create fail\n", subscription_path)
				return err
			}
		}
	}

	return nil
}


func (z *ZK) UpdatePartitionNode(pnode PartitionNode) error {
	path := z.TopicRoot + "/" + pnode.TopicName + "/Partitions/" + pnode.Name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(pnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) UpdateBlockNode(bnode BlockNode) error {
	path := z.TopicRoot + "/" + bnode.TopicName + "/Partitions/" + bnode.PartitionName + "/" + bnode.Name

	ok, _, err := z.conn.Exists(path)
	if !ok {
		return err
	}
	data, err := json.Marshal(bnode)
	if err != nil {
		return err
	}
	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data, sate.Version)
	if err != nil {
		return err
	}

	return nil
}

func (z *ZK) GetPartState(topic_name, part_name string) (PartitionNode, error) {
	var node PartitionNode
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return node, err
	}
	data, _, _ := z.conn.Get(path)

	json.Unmarshal(data, &node)

	return node, nil
}

func (z *ZK) CreateState(name string) error {
	path := z.BrokerRoot + "/" + name + "/state"
	ok, _, err := z.conn.Exists(path)
	logger.DEBUG(logger.DLog, "create broker state %v ok %v\n", path, ok)
	if ok {
		return err
	}
	_, err = z.conn.Create(path, nil, zk.FlagEphemeral, zk.WorldACL(zk.PermAll))
	if err != nil {
		return err
	}
	return nil
}

type Part struct {
	Topic_name    string
	Part_name     string
	BrokerName    string
	BrokHost_Port string
	RaftHost_Port string
	PTP_index     int64
	File_name     string
	Err           string
}

//检查broker是否在线
func (z *ZK) CheckBroker(BrokerName string) bool {
	path := z.BrokerRoot + "/" + BrokerName + "/state"
	ok, _, _ := z.conn.Exists(path)
	logger.DEBUG(logger.DLog, "state(%v) path is %v\n", ok, path)
	return ok
}

//consumer 获取PTP的Brokers //（和PTP的offset）
func (z *ZK) GetBrokers(topic string) ([]Part, error) {
	path := z.TopicRoot + "/" + topic + "/" + "Partitions"
	// var tnode TopicNode
	ok, _, err := z.conn.Exists(path)

	if !ok || err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, err
	}
	var Parts []Part

	partitions, _, _ := z.conn.Children(path)
	for _, part := range partitions {

		PNode, err := z.GetPartitionNode(path + "/" + part)
		if err != nil {
			logger.DEBUG(logger.DError, "get PartitionNode fail%v/%v\n", path, part)
			return nil, err
		}
		PTP_index := PNode.PTPoffset

		var max_dup DuplicateNode
		max_dup.EndOffset = 0
		blocks, _, _ := z.conn.Children(path + "/" + part)
		for _, block := range blocks {
			info, err := z.GetBlockNode(path + "/" + part + "/" + block)
			if err != nil {
				logger.DEBUG(logger.DError, "get block node fail%v/%v/%v\n", part, part, block)
				continue
			}
			logger.DEBUG(logger.DLog, "the block is %v\n", info)
			if info.StartOffset <= PTP_index && info.EndOffset >= PTP_index {

				Duplicates, _, _ := z.conn.Children(path + "/" + part + "/" + info.Name)
				for _, duplicate := range Duplicates {

					duplicatenode, err := z.GetDuplicateNode(path + "/" + part + "/" + info.Name + "/" + duplicate)
					if err != nil {
						logger.DEBUG(logger.DError, "get dup node fail%v/%v/%v/%v\n", path, part, info.Name, duplicate)
						continue
					}
					logger.DEBUG(logger.DLog, "the path of dup is %v node is %v\n", path + "/" + part + "/" + info.Name + "/" + duplicate, duplicatenode)
					if max_dup.EndOffset == 0 || max_dup.EndOffset <= duplicatenode.EndOffset {
						//保证broker在线
						if z.CheckBroker(duplicatenode.BrokerName) {
							max_dup = duplicatenode
						}else{
							logger.DEBUG(logger.DLog, "the broker %v is not online\n", duplicatenode.BrokerName)
						}
					}
				}
				logger.DEBUG(logger.DLog, "the max_dup is %v\n", max_dup)
				var ret string
				if max_dup.EndOffset != 0 {
					ret = "OK"
				} else {
					ret = "thr brokers not online"
				}
				//一个partition只取endoffset最大的broker,其他小的broker副本不全面
				broker, err := z.GetBrokerNode(max_dup.BrokerName)
				if err != nil {
					logger.DEBUG(logger.DError, "get broker node fail %v\n", max_dup.BlockName)
					continue
				}
				Parts = append(Parts, Part{
					Topic_name:    topic,
					Part_name:     part,
					BrokerName:    broker.Name,
					BrokHost_Port: broker.BrokHostPort,
					RaftHost_Port: broker.RaftHostPort,
					PTP_index:     PTP_index,
					File_name:     info.FileName,
					Err:           ret,
				})
				break
			}
		}
	}

	return Parts, nil
}

func (z *ZK) GetBroker(topic, part string, offset int64) (parts []Part, err error) {
	part_path := z.TopicRoot + "/" + topic + "/Partitions/" + part
	ok, _, err := z.conn.Exists(part_path)
	if !ok || err != nil {
		logger.DEBUG(logger.DError, "%v\n", err.Error())
		return nil, err
	}

	var max_dup DuplicateNode
	max_dup.EndOffset = 0
	blocks, _, _ := z.conn.Children(part_path)
	for _, block := range blocks {
		info, err := z.GetBlockNode(part_path + "/" + block)
		if err != nil {
			logger.DEBUG(logger.DError, "get block node fail %v/%v\n", part_path, block)
			continue
		}

		if info.StartOffset <= offset && info.EndOffset >= offset {

			Duplicates, _, _ := z.conn.Children(part_path + "/" + block)
			for _, duplicate := range Duplicates {

				duplicatenode, err := z.GetDuplicateNode(part_path + "/" + block + "/" + duplicate)
				if err != nil {
					logger.DEBUG(logger.DError, "get dup node fail %v/%v/%v\n", part_path, block, duplicate)
				}
				if max_dup.EndOffset == 0 || max_dup.EndOffset <= duplicatenode.EndOffset {
					//保证broker在线
					if z.CheckBroker(duplicatenode.BrokerName) {
						max_dup = duplicatenode
					}
				}
			}
			var ret string
			if max_dup.EndOffset != 0 {
				ret = "OK"
			} else {
				ret = "thr brokers not online"
			}
			//一个partition只取endoffset最大的broker,其他小的broker副本不全面
			broker, err := z.GetBrokerNode(max_dup.BrokerName)
			if err != nil {
				logger.DEBUG(logger.DError, "get Broker node fail %v\n", max_dup.BlockName)
				continue
			}
			parts = append(parts, Part{
				Topic_name:    topic,
				Part_name:     part,
				BrokerName:    broker.Name,
				BrokHost_Port: broker.BrokHostPort,
				RaftHost_Port: broker.RaftHostPort,
				File_name:     info.FileName,
				Err:           ret,
			})
			break
		}
	}
	return parts, nil
}

type StartGetInfo struct {
	Cli_name      string
	Topic_name    string
	PartitionName string
	Option        int8
}

func (z *ZK) CheckSub(info StartGetInfo) bool {

	//检查该consumer是否订阅了该topic或partition

	return true
}

//若Leader不在线，则等待一秒继续请求
func (z *ZK) GetPartNowBrokerNode(topic_name, part_name string) (BrokerNode, BlockNode, int8, error) {
	// 构造获取当前块的路径
	now_block_path := z.TopicRoot + "/" + topic_name + "/" + "Partitions" + "/" + part_name + "/" + "NowBlock"
	
	for {
		// 获取当前块的信息
		NowBlock, err := z.GetBlockNode(now_block_path)
		if err != nil {
			// 如果获取块信息失败，记录错误日志，并返回错误
			logger.DEBUG(logger.DError, "get block node fail, path %v err is %v\n", now_block_path, err.Error())
			return BrokerNode{}, BlockNode{}, 0, err
		}

		// 获取领导 Broker 节点的信息
		Broker, err := z.GetBrokerNode(NowBlock.LeaderBroker)
		if err != nil {
			// 如果获取 Broker 信息失败，记录错误日志，并返回错误
			logger.DEBUG(logger.DError, "get broker node fail, Name %v err is %v\n", NowBlock.LeaderBroker, err.Error())
			return BrokerNode{}, NowBlock, 1, err
		}
		
		// 记录领导 Broker 节点的信息
		logger.DEBUG(logger.DLog, "the Leader Broker is %v\n", NowBlock.LeaderBroker)
		
		// 检查 Broker 是否在线
		ret := z.CheckBroker(Broker.Name)
		
		if ret {
			// 如果 Broker 在线，返回 Broker 节点、块信息和状态码 2
			return Broker, NowBlock, 2, nil
		} else {
			// 如果 Broker 不在线，记录日志并等待 1 秒后重试
			logger.DEBUG(logger.DLog, "the broker %v is not online\n", Broker.Name)
			time.Sleep(time.Second * 1)
		}
	}
}

func (z *ZK) GetBlockSize(topic_name, part_name string) (int, error) {
	path := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return 0, err
	}

	parts, _, err := z.conn.Children(path)
	if err != nil {
		return 0, err
	}
	return len(parts), nil
}

func (z *ZK) GetBrokerNode(name string) (BrokerNode, error) {
	path := z.BrokerRoot + "/" + name
	var bronode BrokerNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return bronode, err
	}
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &bronode)

	return bronode, nil
}

func (z *ZK) GetPartitionNode(path string) (PartitionNode, error) {
	var pnode PartitionNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return pnode, err
	}
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &pnode)

	return pnode, nil
}

func (z *ZK) GetBlockNode(path string) (BlockNode, error) {
	var blocknode BlockNode
	data, _, err := z.conn.Get(path)
	if err != nil {
		logger.DEBUG(logger.DError, "the block path is %v err is %v\n", path, err.Error())
		return blocknode, err
	}
	json.Unmarshal(data, &blocknode)

	return blocknode, nil
}

func (z *ZK) GetDuplicateNodes(topic_name, part_name, block_name string) (nodes []DuplicateNode) {
	BlockPath := z.TopicRoot + "/" + topic_name + "/Partitions/" + part_name + "/" + block_name
	Dups, _, _ := z.conn.Children(BlockPath)

	for _, dup_name := range Dups {
		DupNode, err := z.GetDuplicateNode(BlockPath + "/" + dup_name)
		if err != nil {
			logger.DEBUG(logger.DError, "the dup %v/%vis not exits\n", BlockPath, dup_name)
		} else {
			nodes = append(nodes, DupNode)
		}
	}

	return nodes
}

func (z *ZK) GetDuplicateNode(path string) (DuplicateNode, error) {
	var dupnode DuplicateNode
	ok, _, err := z.conn.Exists(path)
	if !ok {
		return dupnode, err
	}
	data, _, _ := z.conn.Get(path)
	json.Unmarshal(data, &dupnode)

	return dupnode, nil
}

func (z *ZK) DeleteDupNode(TopicName, PartName, BlockName, DupName string) (ret string, err error) {
	path := z.TopicRoot + "/" + TopicName + "/" + "Partitions" + "/" + PartName + "/" + BlockName + "/" + DupName

	_, sate, _ := z.conn.Get(path)
	err = z.conn.Delete(path, sate.Version)
	if err != nil {
		ret = "delete dupnode fail"
	}

	return ret, err
}

func (z *ZK) UpdateDupNode(dnode DuplicateNode) (ret string, err error) {
	path := z.TopicRoot + "/" + dnode.TopicName + "/" + "Partitions" + "/" + dnode.PartitionName + "/" + dnode.BlockName + "/" + dnode.Name

	data_dnode, err := json.Marshal(dnode)
	if err != nil {
		ret = "DupNode turn byte fail"
		return ret, err
	}

	_, sate, _ := z.conn.Get(path)
	_, err = z.conn.Set(path, data_dnode, sate.Version)
	if err != nil {
		ret = "DupNode Update fail"
	}

	return ret, err
}

func (z *ZK) GetPartBlockIndex(TopicName, PartName string) (int64, error) {
	str := z.TopicRoot + "/" + TopicName + "/" + "Partitions" + "/" + PartName
	node, err := z.GetPartitionNode(str)
	if err != nil {
		logger.DEBUG(logger.DError, "get partition node fail path is %v err is %v\n", str, err.Error())
		return 0, err
	}

	return node.Index, nil
}
