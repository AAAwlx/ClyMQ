package clients

import "net"

// PartKey 表示一个分区的关键字信息。
// 包含分区的名称、关联的 Broker 名称、Broker 的主机地址和端口、偏移量以及可能的错误信息。
type PartKey struct {
    Name        string `json:"name"`        // 分区的名称
    Broker_name string `json:"brokername"`  // 关联的 Broker 名称
    Broker_H_P  string `json:"brokerhp"`    // Broker 的主机地址和端口
    Offset      int64  `json:"offset"`      // 偏移量，用于指定从哪个位置开始读取数据
    Err         string `json:"err"`         // 错误信息，表示分区的状态或处理过程中发生的错误
}

// Parts 包含多个分区的关键信息。
// 用于批量处理分区信息。
type Parts struct {
    PartKeys []PartKey `json:"partkeys"` // 分区关键字信息的列表
}

// BrokerInfo 表示 Broker 的信息。
// 包含 Broker 的名称和主机地址及端口。
type BrokerInfo struct {
    Name      string `json:"name"`      // Broker 的名称
    Host_port string `json:"hostport"`  // Broker 的主机地址和端口
}

func GetIpport() string {
	interfaces, err := net.Interfaces()
	ipport := ""
	if err != nil {
		panic("Poor soul, here is what you got: " + err.Error())
	}
	for _, inter := range interfaces {
		mac := inter.HardwareAddr //获取本机MAC地址
		ipport += mac.String()
	}
	return ipport
}
