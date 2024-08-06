package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/logger"
	"ClyMQ/kitex_gen/api/client_operations"
	"ClyMQ/kitex_gen/api/zkserver_operations"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"sync"
	"time"
)

const (
	ALIVE     = "alive"
	DOWN      = "down"
	TOUT   = 60 * 10 //10分钟
	UPDATANUM = 10
)

type Client struct {
	mu       sync.RWMutex
	name     string
	state    string
	consumer client_operations.Client

	// ingroups []*Group
	subList map[string]*SubScription // 若这个consumer关闭则遍历这些订阅并修改
}

func NewClient(ipport string, con client_operations.Client) *Client {
	client := &Client{
		mu:       sync.RWMutex{},
		name:     ipport,
		consumer: con,
		state:    ALIVE,
		subList:  make(map[string]*SubScription),
	}
	return client
}

func (c *Client) CheckConsumer() bool { //心跳检测
	c.mu = sync.RWMutex{}

	for {
		resp, err := c.consumer.Pingpong(context.Background(), &api.PingPongRequest{Ping: true})
		if err != nil || !resp.Pong {
			break
		}

		time.Sleep(time.Second)
	}
	c.mu.Lock()
	c.state = DOWN
	c.mu.Unlock()
	return true
}

func (c *Client) CheckSubscription(sub_name string) bool {
	c.mu.RLock()
	_, ok := c.subList[sub_name]
	c.mu.Unlock()

	return ok
}

func (c *Client) AddSubScription(sub *SubScription) {
	c.mu.Lock()
	c.subList[sub.name] = sub
	c.mu.Unlock()
}

func (c *Client) ReduceSubScription(name string) {
	c.mu.Lock()
	delete(c.subList, name)
	c.mu.Unlock()
}

func (c *Client) GetStat() string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.state
}

func (c *Client) GetCli() *client_operations.Client {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return &c.consumer
}

func (c *Client) GetSub(sub_name string) *SubScription {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.subList[sub_name]
}

type Part struct {
	mu         sync.RWMutex
	topic_name string
	part_name  string
	option     int8
	clis       map[string]*client_operations.Client
	zkclient   *zkserver_operations.Client

	state string
	fd    os.File
	file  *File

	index  int64 //use index to find offset
	offset int64

	start_index int64
	end_index   int64

	buffer_node map[int64]Key
	buffer_msg  map[int64][]Message

	part_had chan Done
	buf_done map[int64]string
}

const (
	OK    = "OK"
	TIOUT = "timeout"

	NOTDO  = "notdo"
	HAVEDO = "havedo"
	HADDO  = "haddo"

	BUFF_NUM  = 5
	AGAIN_NUM = 3
)

type Done struct {
	in   int64
	err  string
	name string
	cli  *client_operations.Client
	// add a consumer name for start to send
}

func NewPart(in info, file *File,  zkclient *zkserver_operations.Client) *Part {

	part := &Part{
		mu:         sync.RWMutex{},
		topic_name: in.topic_name,
		part_name:  in.part_name,
		option:     in.option,
		zkclient: zkclient,

		buffer_node: make(map[int64]Key),
		buffer_msg:  make(map[int64][]Message),
		file:        file,
		clis:        make(map[string]*client_operations.Client),
		state:       DOWN,

		part_had: make(chan Done),
		buf_done: make(map[int64]string),
	}

	part.index = in.offset

	return part
}

// Start 启动分区的处理过程
func (p *Part) Start(close chan *Part) {

	// 打开文件进行读取
	p.fd = *p.file.OpenFileRead() // 获取文件描述符
	offset, err := p.file.FindOffset(&p.fd, p.index) // 查找文件中的偏移量

	if err != nil {
		// 如果查找偏移量失败，记录错误日志
		logger.DEBUG(logger.DError, "%v\n", err.Error())
	}

	// 设置分区的偏移量
	p.offset = offset

	// 加载 BUFF_NUM 个 block 到队列中
	for i := 0; i < BUFF_NUM; i++ {
		err := p.AddBlock()
		if err != nil {
			// 如果添加 block 失败，记录错误日志
			logger.DEBUG(logger.DError, "%v\n", err.Error())
		}
	}

	// 启动 goroutine 处理完成的任务
	go p.GetDone(close)

	p.mu.Lock()
	// 检查分区的状态，如果是 DOWN 状态，则将其设置为 ALIVE
	if p.state == DOWN {
		p.state = ALIVE
	} else {
		// 如果分区已经是 ALIVE 状态，释放锁并记录错误日志
		p.mu.Unlock()
		logger.DEBUG(logger.DError, "the part is ALIVE in before this start\n")
		return
	}

	// 启动 goroutine 发送 block 到各个客户端
	for name, cli := range p.clis {
		go p.SendOneBlock(name, cli)
	}
	p.mu.Unlock()

}

func (p *Part) UpdateClis(cli_names []string, Clis map[string]*client_operations.Client) {
	p.mu.Lock()
	reduce, add := CheckChangeCli(p.clis, cli_names)
	for _, name := range reduce {
		delete(p.clis, name)
		//删除一个consumer，在下一次发送时会检查
		//是否存在，则关闭这个consumer的循环发送
	}

	for _, name := range add {
		p.clis[name] = Clis[name] //新加入一个负责该分片的consumer

		go p.SendOneBlock(name, p.clis[name]) //开启协程，发送消息
	}

	p.mu.Unlock()
}

func (p *Part) AddBlock() error {

	node, msg, err := p.file.ReadFile(&p.fd, p.offset)

	if err != nil {
		return err
	}

	p.mu.Lock()
	p.buf_done[node.Start_index] = NOTDO //新加入的块未被消费
	p.buffer_node[node.Start_index] = node
	p.buffer_msg[node.Start_index] = msg

	p.end_index = node.End_index
	p.offset += int64(NODE_SIZE) + int64(node.Size)

	p.mu.Unlock()

	return err
}

//需要修改，设置未可主动关闭模式，使用管道
func (p *Part) GetDone(close chan *Part) {

	num := 0 // 计数器，用于记录已成功处理的消息数量

	for {
		select {
		case do := <-p.part_had:
			// 从 part_had 通道中接收消息处理结果

			if do.err == OK { // 如果处理成功
				num++

				// 尝试添加新的数据块
				err := p.AddBlock()
				p.mu.Lock() // 获取锁以修改状态

				if err != nil {
					logger.DEBUG(logger.DError, "%v\n", err.Error())
				}

				// 文件消费完成，且该文件不是生产者正在写入的文件
				if p.file.filename != p.part_name+"NowBlock.txt" && err == errors.New("read All file, do not find this index") {
					p.state = DOWN
				}

				// 如果状态为 DOWN 并且缓存中的文件页被消费完后
				if p.state == DOWN && len(p.buf_done) == 0 {
					p.mu.Unlock() // 释放锁
					close <- p   // 向关闭通道发送该 Part 对象
					return
				}

				// 标记缓冲区中的消息为已处理
				p.buf_done[do.in] = HADDO
				in := p.start_index

				for {
					// 检查缓存中是否有连续的已处理消息
					if p.buf_done[in] == HADDO {
						p.start_index = p.buffer_node[in].End_index + 1

						// 更新 Zookeeper 中的 offset
						(*p.zkclient).UpdatePTPOffset(context.Background(), &api.UpdatePTPOffsetRequest{
							Topic: p.topic_name,
							Part: p.part_name,
							Offset: p.start_index,
						})

						// 清理已处理的消息
						delete(p.buf_done, in)
						delete(p.buffer_msg, in)
						delete(p.buffer_node, in)
						in = p.start_index
					} else {
						break
					}
				}

				// 发送处理好的数据块给消费者
				go p.SendOneBlock(do.name, do.cli)

				p.mu.Unlock() // 释放锁
			}

			if do.err == TIOUT { // 如果超时（已尝试发送 3 次）
				p.mu.Lock()
				delete(p.clis, do.name) // 删除超时的消费者
				p.mu.Unlock()
			}

		case <-time.After(TOUT * time.Second): // 超时处理
			close <- p
			return
		}
	}
}

func (p *Part) SendOneBlock(name string, cli *client_operations.Client) {

	var in int64
	in = 0
	num := 0
	for {
		p.mu.Lock()
		if in == 0 {
			in = p.start_index
		}

		if _, ok := p.clis[name]; !ok { //不存在，不再负责这个分片
			p.mu.Unlock()
			return
		}

		if int(in) >= len(p.buf_done) {
			in = 0
		}

		if p.buf_done[in] == NOTDO {

			msg, ok1 := p.buffer_msg[in]
			node, ok2 := p.buffer_node[in]

			if !ok1 || !ok2 {
				logger.DEBUG(logger.DError, "get msg and node from buffer the in = %v\n", in)
			}
			p.buf_done[in] = HAVEDO
			p.mu.Unlock()

			data_msg, _ := json.Marshal(msg)

			for {
				err := p.Pub(cli, node, data_msg)

				if err != nil { //超时等原因
					logger.DEBUG(logger.DError, "%v\n", err.Error())
					num++
					if num >= AGAIN_NUM { //超时三次，将不再向其发送
						p.part_had <- Done{
							in:   node.Start_index,
							err:  TIOUT,
							name: name,
							cli:  cli,
						}

						p.mu.Lock()
						p.buf_done[in] = NOTDO
						p.mu.Unlock()

						break
					}

				} else {
					p.part_had <- Done{
						in:   node.Start_index,
						err:  OK,
						name: name,
						cli:  cli,
					}
					break
				}
			}
			p.mu.Unlock()
			break
		} else {
			in = p.buffer_node[in].End_index + 1
		}
		p.mu.Unlock()
	}

}

// publish 发布
func (p *Part) Pub(cli *client_operations.Client, node Key, data []byte) error {
	resp, err := (*cli).Pub(context.Background(),
		&api.PubRequest{
			TopicName:  p.topic_name,
			PartName:   p.part_name,
			StartIndex: node.Start_index,
			EndIndex:   node.End_index,
			Msg:        data,
		})

	if err != nil || !resp.Ret {
		return err
	}

	/*
	修改zookeeper的该PTP的offset
	*/

	return nil
}

type Group struct {
	rmu        sync.RWMutex
	topic_name string
	consumers  map[string]bool // map[client'name]alive
}

func NewGroup(topic_name, cli_name string) *Group {
	group := &Group{
		rmu:        sync.RWMutex{},
		topic_name: topic_name,
		consumers: make(map[string]bool),
	}
	group.consumers[cli_name] = true
	return group
}

func (g *Group) RecoverClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()

	_, ok := g.consumers[cli_name]
	if ok {
		if g.consumers[cli_name] {
			return errors.New("this client is alive before")
		} else {
			g.consumers[cli_name] = true
			return nil
		}
	} else {
		return errors.New("do not have this client")
	}
}

func (g *Group) AddClient(cli_name string) error {
	g.rmu.Lock()
	defer g.rmu.Unlock()
	_, ok := g.consumers[cli_name]
	if ok {
		return errors.New("this client has in this group")
	} else {
		g.consumers[cli_name] = true
		return nil
	}
}

func (g *Group) DownClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		g.consumers[cli_name] = false
	}
	g.rmu.Unlock()
}

func (g *Group) DeleteClient(cli_name string) {
	g.rmu.Lock()
	_, ok := g.consumers[cli_name]
	if ok {
		delete(g.consumers, cli_name)
	}
	g.rmu.Unlock()
}

type Node struct {
	topic_name string
	part_name  string
	option     int8
	file       *File
	// cli        *client_operations.Client
	fd         os.File

	// index  int64 //use index to find offset
	offset int64

	start_index int64
	// end_index   int64
}

type MSGS struct {
	start_index int64
	end_index   int64
	size        int8
	array       []byte //由[]Message转byte
}

func NewNode(in info, file *File) *Node {
	// logger.DEBUG(logger.DLog, "the file is %v\n", file)
	no := &Node{
		topic_name: in.topic_name,
		part_name:  in.part_name,
		option:     in.option,
		// offset: 	,
		file: file,
	}

	no.fd = *no.file.OpenFileRead()
	no.offset = -1

	return no
}

func (no *Node) ReadMSGS(in info) (MSGS, error) {
	var err error
	var msgs MSGS
	if no.offset == -1 || no.start_index != in.offset {
		no.offset, err = no.file.FindOffset(&no.fd, in.offset)
		if err != nil {
			logger.DEBUG(logger.DLog2, "%v\n", err.Error())
			return MSGS{}, err
		}
	}
	nums := 0
	for nums < int(in.size) {
		node, msg, err := no.file.ReadBytes(&no.fd, no.offset)
		if err != nil {
			if err == io.EOF {
				break
			}else{
				logger.DEBUG(logger.DError, "%v\n", err.Error())
				return MSGS{}, err
			}
		}
		if nums == 0{
			msgs.start_index = node.Start_index
			msgs.end_index   = node.End_index
		}
		nums += int(node.Size)
		no.offset += int64(NODE_SIZE) + int64(node.Size)
		msgs.size = int8(nums)
		msgs.array = append(msgs.array, msg...)
		msgs.end_index = node.End_index
	}

	return msgs, err
}