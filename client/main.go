package main

import (
	"ClyMQ/client/clients"
	"fmt"
	"os"

	// "net"
	"time"
)

func main() {
	// 获取命令行参数中的选项，`option` 表示操作类型（生产者或消费者）
	option := os.Args[1]
	port := ""

	// 检查命令行参数长度，根据参数数量设置端口号
	if len(os.Args) == 3 {
		port = os.Args[2] // 使用传入的端口号
	} else {
		port = "null" // 默认端口号为 "null"
	}

	// 根据 `option` 的值选择运行生产者还是消费者
	switch option {
	case "p": // 生产者
		// 创建一个新的生产者实例，连接到指定的 Zookeeper 地址
		producer, _ := clients.NewProducer("0.0.0.0:2181", "producer1")

		for {
			// 创建一个消息对象，指定主题、分区和消息内容
			msg := clients.Message{
				Topic_name: "phone_number",
				Part_name:  "yclchuxue",
				Msg:        []byte("18788888888"), // 消息内容
			}
			// 向消息代理推送消息，`-1` 表示进行 Raft 同步
			err := producer.Push(msg, -1)
			if err != nil {
				// 如果推送失败，打印错误信息
				fmt.Println(err)
			}

			// 每隔 5 秒钟发送一次消息
			time.Sleep(5 * time.Second)
		}

	case "c": // 消费者
		// 创建一个新的消费者实例，连接到指定的 Zookeeper 地址，并指定端口号
		consumer, _ := clients.NewConsumer("0.0.0.0:2181", "consumer1", port)
		// 启动一个服务器，用于处理发布和双向 RPC 通信
		go consumer.Start_server()

		// 订阅主题和分区，`2` 表示订阅模式
		consumer.Subscription("phone_number", "yclchuxue", 2)

		// 启动消息获取，指定偏移量、主题、分区和选项
		consumer.StartGet(clients.Info{
			Offset:  0,
			Topic:   "phone_number",
			Part:    "yclchuxue",
			Option:  2, // 订阅选项
		})
	}

	// 发送 IP 和端口信息到 broker 服务器，以便服务器能够发布消息到客户端
}

