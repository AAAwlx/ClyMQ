# raft

在消息队列中通过两个协程对raft信息进行处理

其中 StartServer 协程会由 Broker 节点在服务器初始化时开启。

StartServer 函数在消息队列集群中的作用是处理和应用 Raft 日志条目和快照，确保系统的高一致性和可靠性。它通过监听和处理 applyCh 和超时事件，来管理 Raft 状态机的应用和维护，从而支持消息队列系统的稳定和一致操作。

```go
func (p *parts_raft) StartServer() {

	// 输出启动日志，记录当前节点启动的信息
	logger.DEBUG_RAFT(logger.DSnap, "S%d parts_raft start\n", p.me)

	// 启动一个新的 goroutine，执行主循环
	go func() {

		for {
			// 如果当前节点没有被停止
			if !p.killed() {
				select {
				// 监听 applyCh 通道，处理提交的日志条目
				case m := <-p.applyCh:

					// 如果接收到的消息表明当前节点成为了领导者
					if m.BeLeader {
						str := m.TopicName + m.PartName
						// 输出日志，记录成为领导者的信息
						logger.DEBUG_RAFT(logger.DLog, "S%d Broker tPart(%v) become leader aply from %v to %v\n", p.me, str, p.applyindexs[str], m.CommandIndex)
						// 更新应用的索引
						p.applyindexs[str] = m.CommandIndex
						// 如果领导者是当前节点
						if m.Leader == p.me {
							// 向 appench 通道发送信息，表明当前节点是领导者
							p.appench <- info{
								producer:   "Leader",
								topic_name: m.TopicName,
								part_name:  m.PartName,
							}
						}
					// 如果消息是有效的命令且当前节点不是领导者
					} else if m.CommandValid && !m.BeLeader {
						// 记录当前时间
						start := time.Now()

						// 输出日志，尝试获取锁
						logger.DEBUG_RAFT(logger.DLog, "S%d try lock 847\n", p.me)
						// 获取锁
						p.mu.Lock()
						// 输出日志，成功获取锁
						logger.DEBUG_RAFT(logger.DLog, "S%d success lock 847\n", p.me)
						// 计算获取锁的耗时
						ti := time.Since(start).Milliseconds()
						// 输出锁获取耗时的日志
						logger.DEBUG_RAFT(logger.DLog2, "S%d AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA%d\n", p.me, ti)

						// 获取消息中的操作命令
						O := m.Command

						// 检查 CDM（客户端消息字典）中是否有该分区（Tpart），如果没有则创建
						_, ok := p.CDM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "S%d make CDM Tpart(%v)\n", p.me, O.Tpart)
							p.CDM[O.Tpart] = make(map[string]int64)
						}
						// 检查 CSM（客户端状态字典）中是否有该分区（Tpart），如果没有则创建
						_, ok = p.CSM[O.Tpart]
						if !ok {
							logger.DEBUG_RAFT(logger.DLog, "S%d make CSM Tpart(%v)\n", p.me, O.Tpart)
							p.CSM[O.Tpart] = make(map[string]int64)
						}

						// 输出日志，记录当前处理的命令和索引状态
						logger.DEBUG_RAFT(logger.DLog, "S%d TTT CommandValid(%v) applyindex[%v](%v) CommandIndex(%v) CDM[C%v][%v](%v) O.Cmd_index(%v) from(%v)\n", p.me, m.CommandValid, O.Tpart, p.applyindexs[O.Tpart], m.CommandIndex, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, O.Ser_index)

						// 如果当前应用的索引加一等于命令的索引，表明可以应用该命令
						if p.applyindexs[O.Tpart]+1 == m.CommandIndex {

							// 如果客户端名称为 "TIMEOUT"
							if O.Cli_name == "TIMEOUT" {
								// 输出日志并更新应用索引
								logger.DEBUG_RAFT(logger.DLog, "S%d for TIMEOUT update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							// 否则，如果 CDM 中存储的命令索引小于当前命令的索引，表明是新命令
							} else if p.CDM[O.Tpart][O.Cli_name] < O.Cmd_index {
								// 输出日志并更新 CDM 和应用索引
								logger.DEBUG_RAFT(logger.DLeader, "S%d get message update CDM[%v][%v] from %v to %v update applyindex %v to %v\n", p.me, O.Tpart, O.Cli_name, p.CDM[O.Tpart][O.Cli_name], O.Cmd_index, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex

								// 更新 CDM 字典中的命令索引
								p.CDM[O.Tpart][O.Cli_name] = O.Cmd_index
								// 如果操作类型为 "Append"
								if O.Operate == "Append" {
									// 将消息发送到 appench 通道
									p.appench <- info{
										producer:   O.Cli_name,
										message:    O.Msg,
										topic_name: O.Topic,
										part_name:  O.Part,
										size:       O.Size,
									}

									// 尝试将命令索引发送到 Add 通道
									select {
									case p.Add <- COMD{index: m.CommandIndex}:
										// 成功发送到 Add 通道的情况
									default:
										// 无法发送到 Add 通道的情况
									}
								}
							// 否则，命令已经执行过
							} else if p.CDM[O.Tpart][O.Cli_name] == O.Cmd_index {
								// 输出日志，表明命令已经执行过
								logger.DEBUG_RAFT(logger.DLog2, "S%d this cmd had done, the log had two update applyindex %v to %v\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
								p.applyindexs[O.Tpart] = m.CommandIndex
							// 否则，当前命令的索引小于 CDM 中的索引
							} else {
								// 输出日志，表明该命令的索引小于已经应用的命令
								logger.DEBUG_RAFT(logger.DLog2, "S%d the topic_partition(%v) producer(%v) OIndex(%v) < CDM(%v)\n", p.me, O.Tpart, O.Cli_name, O.Cmd_index, p.CDM[O.Tpart][O.Cli_name])
								p.applyindexs[O.Tpart] = m.CommandIndex
							}

						// 如果应用索引与命令索引之间有跳跃
						} else if p.applyindexs[O.Tpart]+1 < m.CommandIndex {
							// 输出警告日志，记录不连续的应用索引和命令索引
							logger.DEBUG_RAFT(logger.DWarn, "S%d the applyindex + 1 (%v) < commandindex(%v)\n", p.me, p.applyindexs[O.Tpart], m.CommandIndex)
						}

						// 如果最大 Raft 状态大小有限制，检查是否需要快照
						if p.maxraftstate > 0 {
							p.CheckSnap()
						}

						// 释放锁
						p.mu.Unlock()
						logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1369\n", p.me)

					} else { // 处理快照的情况
						// 从快照数据中恢复状态
						r := bytes.NewBuffer(m.Snapshot)
						d := raft.NewDecoder(r)
						logger.DEBUG_RAFT(logger.DSnap, "S%d the snapshot applied\n", p.me)
						var S SnapShot
						p.mu.Lock()
						logger.DEBUG_RAFT(logger.DLog, "S%d lock 1029\n", p.me)
						// 解码快照数据，如果解码失败则输出日志并解锁
						if d.Decode(&S) != nil {
							p.mu.Unlock()
							logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1384\n", p.me)
							logger.DEBUG_RAFT(logger.DSnap, "S%d labgob fail\n", p.me)
						} else {
							// 使用快照数据恢复 CDM、CSM 和应用索引
							p.CDM[S.Tpart] = S.Cdm
							p.CSM[S.Tpart] = S.Csm
							logger.DEBUG_RAFT(logger.DSnap, "S%d recover by SnapShot update applyindex(%v) to %v\n", p.me, p.applyindexs[S.Tpart], S.Apliedindex)
							p.applyindexs[S.Tpart] = S.Apliedindex
							p.mu.Unlock()
							logger.DEBUG_RAFT(logger.DLog, "S%d Unlock 1397\n", p.me)
						}
					}

				// 如果没有收到消息，设置超时
				case <-time.After(TIMEOUT * time.Microsecond):
					// 构造一个超时操作并执行
					O := raft.Op{
						Ser_index: int64(p.me),
						Cli_name:  "TIMEOUT",
						Cmd_index: -1,
						Operate:   "TIMEOUT",
					}
					logger.DEBUG_RAFT(logger.DLog, "S%d have log time applied\n", p.me)
					p.mu.RLock()
					// 遍历分区，启动超时操作
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
```

主要功能

1. 启动 Raft 服务器：

   记录日志以表明 Raft 服务器已启动，并启动一个 Goroutine 来处理 Raft 日志和应用消息。
2. 处理来自 applyCh 通道的消息：

3. 成为 Leader：当 ApplyMsg 表示此分区成为 Leader 时，更新相应的 applyindexs。如果当前节点是 Leader，则向 appench 通道发送相关信息。

4. 有效命令处理：处理有效的命令（CommandValid 为 true），检查并更新 CDM（客户端数据映射）和 CSM（客户端状态映射），然后将消息发送到 appench 通道。如果命令的索引比当前 applyindex 大 1，则表示可以处理此命令，并根据命令的操作类型（如 Append）做出相应处理。

5. 快照应用：当接收到快照消息（SnapshotValid 为 true）时，从快照数据中恢复之前的状态，并更新内部状态。
处理超时操作：

6. 定期触发超时操作，向每个分区发出超时日志操作。这个操作可能会触发一些状态更新或其他处理逻辑。
在消息队列集群中的作用
确保一致性：

7. 在 Raft 协议中，Leader 节点需要确保日志的一致性。通过处理来自 applyCh 的消息，Raft 节点可以将日志条目应用到本地状态机，并维护 CDM 和 CSM 的一致性。
命令应用：

   通过检查并更新 applyindexs、CDM 和 CSM，此函数确保每个客户端的操作都被正确地应用，并且不会被重复处理。这对于消息队列中的消息处理至关重要，确保消息不会丢失或重复。

8. 快照恢复：

   快照机制允许 Raft 节点在日志增长过大时恢复状态，从而优化内存使用和加速恢复过程。此函数通过应用快照，确保节点能够从之前的状态中恢复，以便继续处理新的日志条目。

9. 超时处理：

   定期的超时处理有助于在 Raft 集群中触发必要的操作（例如心跳、状态检查等），确保集群的健康和一致性。

同时在 Broker 节点启动时，会启动协程 GetApplych 。该线程会轮询等待 leader 向 applych 中写入要同步的消息。

```go
// 从 applych 通道接收内容，并写入对应的 partition 文件中
func (s *Server) GetApplych(applych chan info) {

    // 遍历从 applych 通道接收的每条消息
	for msg := range applych {

		// 如果消息的生产者是 Leader，则调用 BecomeLeader 方法处理成为 Leader 的逻辑
		if msg.producer == "Leader" {
			s.BecomeLeader(msg) // 成为 leader 进行处理
		} else {
			// 加锁读取当前 Server 的 topics 字典，检查该消息所属的 topic 是否存在
			s.mu.RLock()
			topic, ok := s.topics[msg.topic_name] // 通过 topic_name 查找对应的 topic
			s.mu.RUnlock()
			
			// 打印接收到的消息的调试信息
			logger.DEBUG(logger.DLog, "S%d the message from applych is %v\n", s.me, msg)
			
			// 如果找不到对应的 topic，打印错误日志
			if !ok {
				logger.DEBUG(logger.DError, "topic(%v) is not in this broker\n", msg.topic_name)
			} else {
				// 为消息设置当前节点的相关信息
				msg.me = s.me              // 设置消息来源节点 ID
				msg.BrokerName = s.Name     // 设置当前 broker 名称
				msg.zkclient = &s.zkclient  // 设置 ZooKeeper 客户端
				msg.file_name = "NowBlock.txt" // 设置消息对应的文件名
				
				// 将消息添加到对应的 topic，进行消息的同步或写入
				topic.addMessage(msg) // 信息同步到分区
			}
		}
	}
}
```

其中leader节点写入会通过 raft 中的 Commited 方法向 applych 通道中写入内容。

Commited 方法也是一个在服务器后台执行的协程，该协程由 Make 方法开启。 Make 函数用于初始化 Raft 实例并启动核心组件。

```c

```

该函数中执行了以下功能

* Raft 实例初始化：该函数创建并初始化一个新的 Raft 实例，设置基本状态如节点 ID、日志数组、选举计时器等。
* 随机选举超时：通过设置不同的选举超时，确保集群中的各个节点不会同时发起选举，从而避免冲突。
* 日志和快照恢复：使用持久化存储器恢复之前的日志和快照数据，确保在崩溃恢复时能够保持一致性。
* 后台日志提交协程：启动 Commited 协程，负责将已提交的日志条目应用到状态机。
Raft 心跳或选举超时检测：通过启动 ticker 协程，Raft 节点会定期检查是否需要发起选举或发送心跳。

```go
// Make 函数用于初始化 Raft 实例并启动核心组件
// peers: Raft 集群中的其他节点
// me: 当前节点的 ID
// persister: 用于保存和恢复 Raft 状态的持久化对象
// applyCh: 用于向上层服务发送应用消息的通道
// topic_name, part_name: 当前节点处理的主题和分区名称
func Make(peers []*raft_operations.Client, me int,
	persister *Persister, applyCh chan ApplyMsg, topic_name, part_name string) *Raft {
    
    // 创建 Raft 实例并初始化基本字段
	rf := &Raft{}
	rf.peers = peers                           // 集群中的所有节点
	rf.persister = persister                   // 持久化对象，用于保存和恢复 Raft 状态
	rf.me = me                                 // 当前节点的 ID
	rf.votedFor = -1                           // 表示当前节点未投票
	rf.leaderId = -1                           // 初始化 Leader ID 为 -1
	rf.currentTerm = 0                         // 当前节点的任期初始化为 0
	rf.electionElapsed = 0                     // 选举超时计数器，初始为 0
	rf.mu = sync.Mutex{}                       // 互斥锁，保护并发操作
	rf.topic_name = topic_name                 // 当前节点处理的主题名称
	rf.part_name = part_name                   // 当前节点处理的分区名称

	// 设置随机种子，确保选举超时时间不同
	rand.Seed(time.Now().UnixNano())

	// 随机生成选举超时时间，介于 300-500 毫秒之间
	rf.electionRandomTimeout = rand.Intn(200) + 300

	rf.state = 0                               // 初始化为 Follower 状态
	// rf.cond = sync.NewCond(&rf.mu)            // 可选条件变量（暂时注释掉）
	rf.log = []LogNode{}                       // 初始化日志数组
	rf.X = 0                                   // X 表示快照截断的日志索引

	// 将初始日志条目 (任期为 0) 加入日志数组
	rf.log = append(rf.log, LogNode{
		Logterm: 0,
	})

	// 初始化每个节点的 nextIndex 和 matchIndex
	for i := 0; i < len(peers); i++ {
		rf.nextIndex = append(rf.nextIndex, 1)  // 下一个要发送给每个节点的日志索引
		rf.matchIndex = append(rf.matchIndex, 0) // 每个节点已知的匹配日志索引
	}

	// 初始化 Raft 的提交索引和应用索引
	rf.commitIndex = 0    // 已提交的最大日志索引
	rf.lastApplied = 0    // 已应用的最大日志索引
	rf.tindex = 0         // 用于跟踪投票的索引
	startindex := rf.X    // 起始日志索引

	// 启动日志提交的后台协程
	go rf.Commited(startindex, applyCh)

	// 初始化日志系统，可能是 Raft 的调试和日志记录模块
	LOGinit()

	// 恢复持久化的 Raft 状态和快照
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	// 启动 Raft 心跳检测或选举超时的计时器
	go rf.ticker()

	// 返回初始化好的 Raft 实例
	return rf
}
```

Make方法会在设置分区信息的功能中被逐级调用到。

如果生产者将该 info 中的 option 设置为 -1 则将该分片设置为raft同步信息的模式。在设置该模式后会创建与这个分区相对应的raft实例，用于进行 broker 间的消息同步与恢复。

```go
ZkServer.SetPartitionState -> rpcServer..AddRaftPartition -> Server.AddRaftHandle -> parts_rafts.AddPart_Raft -> raft.Make
```
