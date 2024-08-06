package clients

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/server_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"context"
	"errors"
	"sync"

	"github.com/cloudwego/kitex/client"
)

type Producer struct {
    mu sync.RWMutex // 读写互斥锁，用于同步对 Producer 数据的并发访问。
    
    Name            string                         // 生产者的名称。
    ZkBroker        zkserver_operations.Client     // 用于与 ZooKeeper 进行操作的客户端。
    Topic_Partions  map[string]server_operations.Client // 主题和分区的映射，键是主题和分区名称的组合（例如 "topicname+partname"），值是负责这些分区的rpc客户端。
    Top_Part_indexs map[string]int64               // 主题名称到分区索引的映射，键是主题名称，值是该主题的分区索引。
}

//生产者向消息队列发送的信息
type Message struct {
	Topic_name string
	Part_name  string
	Msg        []byte
}

func NewProducer(zkbroker string, name string) (*Producer, error) {
	P := Producer{
		mu:             sync.RWMutex{},
		Name:           name,
		Topic_Partions: make(map[string]server_operations.Client),
		Top_Part_indexs: make(map[string]int64),
	}
	var err error
	P.ZkBroker, err = zkserver_operations.NewClient(P.Name, client.WithHostPorts(zkbroker))

	return &P, err
}

//创建一个主题
func (p *Producer) CreateTopic(topic_name string) error {

	resp, err := p.ZkBroker.CreateTopic(context.Background(), &api.CreateTopicRequest{
		TopicName: topic_name,
	})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

//创建一个消息分区
func (p *Producer) CreatePart(topic_name, part_name string) error {
	resp, err := p.ZkBroker.CreatePart(context.Background(), &api.CreatePartRequest{
		TopicName: topic_name,
		PartName:  part_name,
	})

	if err != nil || !resp.Ret {
		return err
	}

	return nil
}

// SetPartitionState 设置指定主题和分区的状态。
func (p *Producer) SetPartitionState(topic_name, part_name string, option, dupnum int8) error {
    // 向 ZooKeeper 发送请求以设置分区状态
    resp, err := p.ZkBroker.SetPartitionState(context.Background(), &api.SetPartitionStateRequest{
        Topic:     topic_name, // 主题名称
        Partition: part_name, // 分区名称
        Option:    option,    // 状态选项
        Dupnum:    dupnum,    // 副本数量
    })

    // 如果发生错误或响应返回的 Ret 字段为 false，返回错误
    if err != nil || !resp.Ret {
        return err
    }
    
    // 操作成功，返回 nil
    return nil
}

//将消息推送到指定的分区，并根据需要进行处理
func (p *Producer) Push(msg Message, ack int) error {
    // 构造主题和分区的唯一标识符
    str := msg.Topic_name + msg.Part_name
    var ok2 bool
    var index int64

    // 获取读锁，读取 Topic_Partions 和 Top_Part_indexs
    p.mu.RLock()
    cli, ok1 := p.Topic_Partions[str] // 获取分区对应的客户端
    if ack == -1 { // 如果使用 Raft 模式，获取当前索引
        index, ok2 = p.Top_Part_indexs[str]
    }
    zk := p.ZkBroker // ZooKeeper 客户端
    p.mu.RUnlock()

    // 如果分区的rpc客户端不存在
    if !ok1 {
        // 从 ZooKeeper 获取分区所在的 Broker 信息
        resp, err := zk.ProGetBroker(context.Background(), &api.ProGetBrokRequest{
            TopicName: msg.Topic_name, // 主题名称
            PartName:  msg.Part_name,  // 分区名称
        })

        // 如果发生错误或响应失败，返回错误
        if err != nil || !resp.Ret {
            return err
        }

        // 创建新的rpc客户端实例
        cli, err = server_operations.NewClient(p.Name, client.WithHostPorts(resp.BrokerHostPort))

        // 如果创建客户端时发生错误，返回错误
        if err != nil {
            return err
        }

        // 获取写锁，将新的客户端添加到 Topic_Partions 中
        p.mu.Lock()
        p.Topic_Partions[str] = cli
        p.mu.Unlock()
    }

    // 如果使用 Raft 模式且索引不存在
    if ack == -1 {
        if !ok2 {
            p.mu.Lock()
            p.Top_Part_indexs[str] = 0 // 初始化索引
            p.mu.Unlock()
        }
    }

    // 推送消息到分区
    resp, err := cli.Push(context.Background(), &api.PushRequest{
        Producer: p.Name,        // 生产者名称
        Topic:    msg.Topic_name, // 主题名称
        Key:      msg.Part_name,  // 分区名称
        Message:  msg.Msg,        // 消息内容
        Ack:      int8(ack),      // 确认模式
        Cmdindex: index,          // Raft 模式下的索引
    })

    // 处理推送响应
    if err == nil && resp.Ret {
        // 更新索引并释放写锁
        p.mu.Lock()
        p.Top_Part_indexs[str] = index + 1
        p.mu.Unlock()
        return nil
    } else if resp.Err == "partition remove" {
        // 如果分区已被移除，删除旧的客户端并重新推送消息
        p.mu.Lock()
        delete(p.Topic_Partions, str)
        p.mu.Unlock()
        return p.Push(msg, ack) // 递归调用以重新推送消息
    } else {
        // 其他错误情况
        return errors.New("err != " + err.Error() + "or resp.Ret == false")
    }
}