# zookeeper

## zk服务器中的功能

注册zookeeper中的节点：

```go
// RegisterNode 将指定的节点信息注册到 ZooKeeper。
// 参数:
//   - znode: 要注册的节点对象，类型可以是 BrokerNode、TopicNode、PartitionNode、BlockNode、DuplicateNode 或 SubscriptionNode。
// 返回值:
//   - err: 如果在处理或注册节点时发生错误，则返回相应的错误信息。
func (z *ZK) RegisterNode(znode interface{}) (err error)
```


```go
//更新Partition
func (z *ZK) UpdatePartitionNode(pnode PartitionNode)
```

