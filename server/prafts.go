package server

import (
	"ClyMQ/kitex_gen/api"
	"ClyMQ/kitex_gen/api/raft_operations"
	"ClyMQ/logger"
	"ClyMQ/raft"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/server"
)

const (
	// TIMEOUT 定义了超时时间的常量。单位是微秒（百万分之一秒）。
	// 这个值为 1000 * 1000 微秒，也就是 1 秒。
	TIMEOUT = 1000 * 1000
	
	// ErrNoKey 定义了一个常量字符串，表示“没有这个键”的错误信息。
	ErrNoKey = "ErrNoKey"
	
	// ErrWrongGroup 定义了一个常量字符串，表示“错误的组”的错误信息。
	ErrWrongGroup = "ErrWrongGroup"
	
	// ErrWrongLeader 定义了一个常量字符串，表示“错误的领导者”的错误信息。
	ErrWrongLeader = "ErrWrongLeader"
	
	// ErrTimeOut 定义了一个常量字符串，表示“超时”的错误信息。
	ErrTimeOut = "ErrTimeOut"
	
	// ErrWrongNum 定义了一个常量字符串，表示“错误的数字”的错误信息。
	ErrWrongNum = "ErrWrongNum"
)


// type Op struct {
// 	Cli_name string //client的唯一标识
// 	Cmd_index int64  //操作id号
// 	Ser_index int64  //Server的id
// 	Operate   string //这里的操作只有append
// 	Tpart     string //这里的shard为topic+partition
// 	Topic     string
// 	Part      string
// 	Num       int
// 	// KVS       map[string]string     //我们将返回的start直接交给partition，写入文件中
// 	// CSM map[string]int64
// 	// CDM map[string]int64

// 	Msg  []byte
// 	Size int8
// }

type COMD struct {
	index int
	// csm     map[string]int64
	// cdm     map[string]int64

	// num     int
	The_num int
}

type SnapShot struct {
	// Kvs    []map[string]string
	Tpart string
	// topic 	string
	// part 	string
	Csm map[string]int64
	Cdm map[string]int64
	// KVSMAP map[int]int
	Apliedindex int
}

//用于 Raft 协议的实现和管理，为 Raft 的状态管理、日志处理和命令处理提供支持
type parts_raft struct {
	// mu 是一个读写锁，用于保护并发访问的数据结构。
	mu sync.RWMutex

	// srv_raft 是一个 Raft 服务器实例，用于与 Raft 协议相关的操作。
	srv_raft server.Server
	
	// Partitions 存储每个分区的 Raft 实例，键是分区的标识符，值是 Raft 实例指针。
	Partitions map[string]*raft.Raft
	
	// Leaders 记录每个分区的领导者状态，键是分区的标识符，值是布尔值，表示是否是领导者。
	Leaders map[string]bool

	// me 是当前节点的标识符。
	me int
	
	// appench 是一个用于接收待追加日志的消息的通道。
	appench chan info
	
	// applyCh 是一个用于接收 Raft 应用消息的通道。
	applyCh chan raft.ApplyMsg

	// maxraftstate 定义了 Raft 日志的最大大小。如果日志增长到这个大小，则进行快照。
	maxraftstate int
	
	// dead 标志位，用于标记节点是否已经终止。
	dead int32
	
	// Add 是一个用于接收命令的通道。
	Add chan COMD

	// CSM 记录生产者状态，键是主题名称，值是一个映射，表示每个分区的最新偏移量。
	CSM map[string]map[string]int64
	
	// CDM 记录消费者状态，键是主题名称，值是一个映射，表示每个分区的最新偏移量。
	CDM map[string]map[string]int64

	// ChanComd 是一个消息队列，用于管道化获取键值对的命令。
	ChanComd map[int]COMD

	// applyindexs 是一个映射，记录每个分区的应用日志索引。
	applyindexs map[string]int
	
	// Now_Num 记录当前的序号或计数。
	Now_Num int
	
	// check 是一个布尔值，用于某些检查或标志，但在这个片段中未使用。
	// check bool
}


func NewParts_Raft() *parts_raft {
	return &parts_raft{
		mu: sync.RWMutex{},
	}
}

func (p *parts_raft) Make(name string, opts []server.Option, appench chan info, me int) {

	p.appench = appench
	p.me = me
	p.applyCh = make(chan raft.ApplyMsg)
	p.Add = make(chan COMD)

	p.CDM = make(map[string]map[string]int64)
	p.CSM = make(map[string]map[string]int64)
	p.Partitions = make(map[string]*raft.Raft)
	p.applyindexs = make(map[string]int)
	p.Leaders = make(map[string]bool)

	// logger.DEBUG_RAFT(logger.DLog, "this raft host port is %v\n", host_port)
	// addr, _ := net.ResolveIPAddr("tcp", host_port)
	// var opts []server.Option
	// opts = append(opts, server.WithServiceAddr(addr))
	// logger.DEBUG_RAFT(logger.DLog, "the opt %v\n", opts)
	srv_raft := raft_operations.NewServer(p, opts...)
	p.srv_raft = srv_raft

	err := srv_raft.Run()
	if err != nil {
		logger.DEBUG_RAFT(logger.DError, "the raft run fail %v\n", err.Error())
	}
}

func (p *parts_raft) Append(in info) (string, error) {
	// 构造分区标识符
	str := in.topic_name + in.part_name
	
	// 记录日志，显示收到的消息和命令索引
	logger.DEBUG_RAFT(logger.DLeader, "S%d <-- C%v putappend message(%v) topic_partition(%v)\n", p.me, in.producer, in.cmdindex, str)

	// 加锁，开始对内部数据结构的保护
	p.mu.Lock()
	
	// 检查当前分区是否在本 broker 中
	_, ok := p.Partitions[str]
	if !ok {
		ret := "this partition is not in this broker"
		logger.DEBUG_RAFT(logger.DLog, "this partition(%v) is not in this broker\n", str)
		p.mu.Unlock() // 释放锁
		time.Sleep(200 * time.Millisecond) // 延时处理
		return ret, errors.New(str)
	}
	
	// 获取分区的领导者状态,保证只有 leader 节点可写
	_, isLeader := p.Partitions[str].GetState()
	if !isLeader {
		logger.DEBUG_RAFT(logger.DLog, "S%d this is not leader\n", p.me)
		p.mu.Unlock() // 释放锁
		time.Sleep(200 * time.Millisecond) // 延时处理
		return ErrWrongLeader, nil
	}

	// 检查应用日志的索引是否为 0
	if p.applyindexs[str] == 0 {
		logger.DEBUG_RAFT(logger.DLog, "S%d the snap not applied applyindex is %v\n", p.me, p.applyindexs[str])
		p.mu.Unlock() // 释放锁
		time.Sleep(200 * time.Millisecond) // 延时处理
		return ErrTimeOut, nil
	}

	// 检查 CDM 和 CSM 中是否存在当前分区的记录，如果不存在则初始化
	_, ok = p.CDM[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DLog, "S%d make CDM Tpart(%v)\n", p.me, str)
		p.CDM[str] = make(map[string]int64)
	}
	_, ok = p.CSM[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DLog, "S%d make CSM Tpart(%v)\n", p.me, str)
		p.CSM[str] = make(map[string]int64)
	}

	// 检查 CDM 中是否存在生产者的记录
	in1, okk1 := p.CDM[str][in.producer]
	if okk1 && in1 == in.cmdindex {
		logger.DEBUG_RAFT(logger.DInfo, "S%d p.CDM[%v][%v](%v) in.cmdindex(%v)\n", p.me, str, in.producer, p.CDM[str][in.producer], in.cmdindex)
		p.mu.Unlock() // 释放锁
		return OK, nil
	} else if !okk1 {
		logger.DEBUG_RAFT(logger.DLog, "S%d add CDM[%v][%v](%v)\n", p.me, str, in.producer, 0)
		p.CDM[str][in.producer] = 0
	}
	p.mu.Unlock() // 释放锁

	// 创建 Raft 操作对象
	var index int
	O := raft.Op{
		Ser_index: int64(p.me),
		Cli_name:  in.producer,
		Cmd_index: in.cmdindex,
		Operate:   "Append",
		Topic:     in.topic_name,
		Part:      in.part_name,
		Tpart:     str,
		Msg:       in.message,
		Size:      in.size,
	}

	// 再次加锁，更新 CSM 中的记录
	p.mu.Lock()
	logger.DEBUG_RAFT(logger.DLog, "S%d lock 285\n", p.me)
	in2, okk2 := p.CSM[str][in.producer]
	if !okk2 {
		logger.DEBUG_RAFT(logger.DLog, "S%d add CSM[%v][%v](%v)\n", p.me, str, in.producer, 0)
		p.CSM[str][in.producer] = 0
	}
	p.mu.Unlock() // 释放锁

	logger.DEBUG_RAFT(logger.DInfo, "S%d p.CSM[%v][%v](%v) in.cmdindex(%v)\n", p.me, str, in.producer, p.CSM[str][in.producer], in.cmdindex)
	if in2 == in.cmdindex {
		_, isLeader = p.Partitions[str].GetState()
	} else {
		index, _, isLeader = p.Partitions[str].Start(O, false, 0)
	}

	if !isLeader {
		return ErrWrongLeader, nil
	} else {
		for {
			select {
				// 监听 p.Add 通道中的消息
				case out := <-p.Add:
					// 在处理来自 p.Add 通道的消息时，先获取锁以保护共享状态
					p.mu.Lock()
					logger.DEBUG_RAFT(logger.DLog, "S%d lock 312\n", p.me)
			
					// 更新 CSM（Command Status Map）中的偏移量，标记当前 producer 的 cmdindex
					p.CSM[str][in.producer] = in.cmdindex
					p.mu.Unlock() // 释放锁
					
					// 检查操作的索引是否与收到的消息的索引匹配
					if index == out.index {
						return OK, nil // 如果匹配，返回成功
					} else {
						logger.DEBUG_RAFT(logger.DLog, "S%d index != out.index pytappend %d != %d\n", p.me, index, out.index)
					}
			
				// 监听超时事件
				case <-time.After(TIMEOUT * time.Microsecond):
					// 超时处理，检查当前节点是否为领导者
					_, isLeader := p.Partitions[str].GetState()
					ret := ErrTimeOut // 默认返回超时错误
					p.mu.Lock()
					lastindex, ok := p.CSM[str][in.producer]
					if !ok {
						p.CSM[str][in.producer] = 0 // 如果没有找到状态，初始化为 0
					}
					logger.DEBUG_RAFT(logger.DLog, "S%d lock 332\n", p.me)
					logger.DEBUG_RAFT(logger.DLeader, "S%d time out\n", p.me)
					
					// 如果当前节点不是领导者，设置错误返回值为 ErrWrongLeader
					if !isLeader {
						ret = ErrWrongLeader
						p.CSM[str][in.producer] = lastindex // 恢复 CSM 中的状态
					}
					p.mu.Unlock() // 释放锁
					
					return ret, nil // 返回错误状态
			}
		}
	}
}

//some problem
func (p *parts_raft) Kill(str string) {
	atomic.StoreInt32(&p.dead, 1)
	logger.DEBUG_RAFT(logger.DLog, "S%d kill\n", p.me)
	// p.Partitions[str].Kill()
	// Your code here, if desired.
}

func (p *parts_raft) killed() bool {
	z := atomic.LoadInt32(&p.dead)
	return z == 1
}

func (p *parts_raft) SendSnapShot(str string) {
	w := new(bytes.Buffer)
	e := raft.NewEncoder(w)
	S := SnapShot{
		Csm:   p.CSM[str],
		Cdm:   p.CDM[str],
		Tpart: str,
		// Rpcindex:    kv.rpcindex,
		Apliedindex: p.applyindexs[str],
	}
	e.Encode(S)
	logger.DEBUG_RAFT(logger.DSnap, "S%d the size need to snap\n", p.me)
	data := w.Bytes()
	go p.Partitions[str].Snapshot(S.Apliedindex, data)
	// X, num := kv.rf.RaftSize()
	// fmt.Println("S", kv.me, "raftsize", num, "snap.lastindex.X", X)
}

func (p *parts_raft) CheckSnap() {
	// kv.mu.Lock()

	for str, raft := range p.Partitions {
		X, num := raft.RaftSize()
		logger.DEBUG_RAFT(logger.DSnap, "S%d the size is (%v) applidindex(%v) X(%v)\n", p.me, num, p.applyindexs[str], X)
		if num >= int(float64(p.maxraftstate)) {
			if p.applyindexs[str] == 0 || p.applyindexs[str] <= X {
				// kv.mu.Unlock()
				return
			}
			p.SendSnapShot(str)
		}
	}
	// kv.mu.Unlock()
}

func (p *parts_raft) StartServer() {

	logger.DEBUG_RAFT(logger.DSnap, "S%d parts_raft start\n", p.me)

	// logger.LOGinit()

	go func() {

		for {
			if !p.killed() {
				select {
				case m := <-p.applyCh:

					if m.BeLeader {
						str := m.TopicName + m.PartName
						logger.DEBUG_RAFT(logger.DLog, "S%d Broker tPart(%v) become leader aply from %v to %v\n", p.me, str, p.applyindexs[str], m.CommandIndex)
						p.applyindexs[str] = m.CommandIndex
						if m.Leader == p.me {
							p.appench <- info{
								producer:   "Leader",
								topic_name: m.TopicName,
								part_name:  m.PartName,
							}
						}
					} else if m.CommandValid && !m.BeLeader {
						start := time.Now()

						logger.DEBUG_RAFT(logger.DLog, "S%d try lock 847\n", p.me)
						p.mu.Lock()
						logger.DEBUG_RAFT(logger.DLog, "S%d success lock 847\n", p.me)
						ti := time.Since(start).Milliseconds()
						logger.DEBUG_RAFT(logger.DLog2, "S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", p.me, ti)

						O := m.Command

						_, ok := p.CDM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "S%d make CDM Tpart(%v)\n", p.me, O.Tpart)
							p.CDM[O.Tpart] = make(map[string]int64)
							// if O.Cli_name != "TIMEOUT" {
							// 	p.CDM[]
							// }
						}
						_, ok = p.CSM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "S%d make CSM Tpart(%v)\n", p.me, O.Tpart)
							p.CSM[O.Tpart] = make(map[string]int64)
						}

						logger.DEBUG_RAFT(logger.DLog, "S%d TTT CommandValid(%v) applyindex[%v](%v) CommandIndex(%v) CDM[C%v][%v](%v) O.Cmd_index(%v) from(%v)\n", p.me, m.CommandValid, O.Tpart, p.applyindexs[O.Tpart], m.CommandIndex, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, O.Ser_index)

						if p.applyindexs[O.Tpart]+1 == m.CommandIndex {

							if O.Cli_name == "TIMEOUT" {
								logger.DEBUG_RAFT(logger.DLog, "S%d for TIMEOUT update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							} else if p.CDM[O.Tpart][O.Cli_name] < O.Cmd_index {
								logger.DEBUG_RAFT(logger.DLeader, "S%d get message update CDM[%v][%v] from %v to %v update applyindex %v to %v\n", p.me, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex

								p.CDM[O.Tpart][O.Cli_name] = O.Cmd_index
								if O.Operate == "Append" {

									p.appench <- info{
										producer:   O.Cli_name,
										message:    O.Msg,
										topic_name: O.Topic,
										part_name:  O.Part,
										size:       O.Size,
									}

									select {
									case p.Add <- COMD{index: m.CommandIndex}:
										// logger.DEBUG_RAFT(logger.DLog, "S%d write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									default:
										// logger.DEBUG_RAFT(logger.DLog, "S%d can not write putAdd in(%v)\n", kv.me, kv.gid, m.CommandIndex)
									}
								}
							} else if p.CDM[O.Tpart][O.Cli_name] == O.Cmd_index {
								logger.DEBUG_RAFT(logger.DLog2, "S%d this cmd had done, the log had two update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							} else {
								logger.DEBUG_RAFT(logger.DLog2, "S%d the topic_partition(%v) producer(%v) OIndex(%v) < CDM(%v)\n", p.me, O.Tpart, O.Cli_name, O.Cmd_index, p.CDM[O.Tpart][O.Cli_name])
								p.applyindexs[O.Tpart] = m.CommandIndex
							}

						} else if p.applyindexs[O.Tpart]+1 < m.CommandIndex {
							logger.DEBUG_RAFT(logger.DWarn, "S%d the applyindex + 1 (%v) < commandindex(%v)\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
							// kv.applyindex = m.CommandIndex
						}

						if p.maxraftstate > 0 {
							p.CheckSnap()
						}

						p.mu.Unlock()
						logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1369\n", p.me)

						// if maxraftstate > 0 {
						// 	go kv.CheckSnap()
						// }

					} else { //read snapshot
						r := bytes.NewBuffer(m.Snapshot)
						d := raft.NewDecoder(r)
						logger.DEBUG_RAFT(logger.DSnap, "S%d the snapshot applied\n", p.me)
						var S SnapShot
						p.mu.Lock()
						logger.DEBUG_RAFT(logger.DLog, "S%d lock 1029\n", p.me)
						if d.Decode(&S) != nil {
							p.mu.Unlock()
							logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1384\n", p.me)
							logger.DEBUG_RAFT(logger.DSnap, "S%d labgob fail\n", p.me)
						} else {
							p.CDM[S.Tpart] = S.Cdm
							p.CSM[S.Tpart] = S.Csm
							// kv.config = S.Config
							// kv.rpcindex = S.Rpcindex
							// kv.check = false
							logger.DEBUG_RAFT(logger.DSnap, "S%d recover by SnapShot update applyindex(%v) to %v\n", p.me, p.applyindexs[S.Tpart], S.Apliedindex)
							p.applyindexs[S.Tpart] = S.Apliedindex
							p.mu.Unlock()
							logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1397\n", p.me)
						}

					}

				case <-time.After(TIMEOUT * time.Microsecond):
					O := raft.Op{
						Ser_index: int64(p.me),
						Cli_name:  "TIMEOUT",
						Cmd_index: -1,
						Operate:   "TIMEOUT",
					}
					logger.DEBUG_RAFT(logger.DLog, "S%d have log time applied\n", p.me)
					p.mu.RLock()
					for str, raft := range p.Partitions {
						O.Tpart = str
						raft.Start(O, false, 0)
					}
					p.mu.RUnlock()
				}
			}
		}

	}()
}

//检查或创建一个raft
//添加一个需要raft同步的partition
func (p *parts_raft) AddPart_Raft(peers []*raft_operations.Client, me int, topic_name, part_name string, appendch chan info) {

	//启动一个raft，即调用Make(), 需要提供各节点broker的 raft_clients, 和该partition的管道，
	str := topic_name + part_name
	p.mu.Lock()
	_, ok := p.Partitions[str]
	if !ok {
		per := &raft.Persister{}
		part_raft := raft.Make(peers, me, per, p.applyCh, topic_name, part_name)
		p.Partitions[str] = part_raft
	}
	p.mu.Unlock()
}

func (p *parts_raft) CheckPartState(TopicName, PartName string) bool {
	str := TopicName + PartName
	p.mu.Lock()
	defer p.mu.Unlock()

	_, ok := p.Partitions[str]
	return ok
}

func (p *parts_raft) DeletePart_raft(TopicName, PartName string) error {
	str := TopicName + PartName

	p.mu.Lock()
	defer p.mu.Unlock()
	raft, ok := p.Partitions[str]
	if !ok {
		logger.DEBUG_RAFT(logger.DError, "this tpoic-partition(%v) is not in this broker\n", str)
		return errors.New("this tpoic-partition is not in this broker")
	} else {
		raft.Kill()
		delete(p.Partitions, str)
		return nil
	}
}

func (p *parts_raft) RequestVote(ctx context.Context, rep *api.RequestVoteArgs_) (r *api.RequestVoteReply, err error) {
	str := rep.TopicName + rep.PartName
	p.mu.RLock()
	raft_ptr, ok := p.Partitions[str]
	p.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DWarn, "raft(%v) is not get\n", str)
		time.Sleep(time.Second * 10)
		return
	}

	resp := raft_ptr.RequestVote(&raft.RequestVoteArgs{
		Term:         int(rep.Term),
		CandidateId:  int(rep.CandidateId),
		LastLogIndex: int(rep.LastLogIndex),
		LastLogIterm: int(rep.LastLogIterm),
	})

	return &api.RequestVoteReply{
		Term:        int8(resp.Term),
		VoteGranted: resp.VoteGranted,
	}, nil
}

func (p *parts_raft) AppendEntries(ctx context.Context, rep *api.AppendEntriesArgs_) (r *api.AppendEntriesReply, err error) {
	str := rep.TopicName + rep.PartName
	var array []raft.LogNode
	json.Unmarshal(rep.Entries, &array)

	p.mu.RLock()
	raft_ptr, ok := p.Partitions[str]
	p.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DWarn, "raft(%v) is not get\n", str)
		time.Sleep(time.Second * 10)
		return
	}

	resp := raft_ptr.AppendEntries(&raft.AppendEntriesArgs{
		Term:         int(rep.Term),
		LeaderId:     int(rep.LeaderId),
		PrevLogIndex: int(rep.PrevLogIndex),
		PrevLogIterm: int(rep.PrevLogIterm),
		LeaderCommit: int(rep.LeaderCommit),
		Entries:      array,
	})

	return &api.AppendEntriesReply{
		Success:        resp.Success,
		Term:           int8(resp.Term),
		Logterm:        int8(resp.Logterm),
		Termfirstindex: int8(resp.Termfirstindex),
	}, nil
}

func (p *parts_raft) SnapShot(ctx context.Context, rep *api.SnapShotArgs_) (r *api.SnapShotReply, err error) {
	str := rep.TopicName + rep.PartName
	p.mu.RLock()
	raft_ptr, ok := p.Partitions[str]
	p.mu.RUnlock()
	if !ok {
		logger.DEBUG(logger.DWarn, "raft(%v) is not get\n", str)
		time.Sleep(time.Second * 10)
		return
	}

	resp := raft_ptr.InstallSnapshot(&raft.SnapShotArgs{
		Term:              int(rep.Term),
		LeaderId:          int(rep.LeaderId),
		LastIncludedIndex: int(rep.LastIncludedIndex),
		LastIncludedTerm:  int(rep.LastIncludedTerm),
		// Log:               rep.Log,
		Snapshot: rep.Snapshot,
	})

	return &api.SnapShotReply{
		Term: int8(resp.Term),
	}, nil
}

func (p *parts_raft) Pingpongtest(ctx context.Context, rep *api.PingPongArgs_) (r *api.PingPongReply, err error) {

	return &api.PingPongReply{
		Pong: true,
	}, nil
}
