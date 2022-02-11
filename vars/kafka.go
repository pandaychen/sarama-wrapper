package vars

// 测试配置
const (
	HOST = "127.0.0.1:9092"
	// Topic 注: 如果关闭了自动创建分区，使用前都需要手动创建对应分区
	Topic                  = "standAlone"
	Topic2                 = "consumerGroup"
	Topic3                 = "benchmark"
	TopicPartition         = "partition"
	TopicCompression       = "compression"
	DefaultPartition       = 0
	DefaultConsumerGroupID = "defaultgid"

	DefaultstandaloneConsumerGroupID = "standaloneid"
)
