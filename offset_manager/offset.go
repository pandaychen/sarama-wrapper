package offset_manager

/*
   注意：仅适用于standalone 消费者
   本例展示最简单的 偏移量管理器 的手动使用（在 消费者组中 sarama 库实现了 offset 自动管理）
   增加偏移量管理后就可以记录下每次消费的位置，便于下次接着消费，避免 sarama.OffsetOldest 的重复消费或者 sarama.OffsetNewest 漏掉部分消息
   NOTE: 相比普通 consumer 增加了 OffsetManager，调用 MarkOffset 手动记录了当前消费的 offset，最后调用 commit 提交到 kafka。
   sarama 库的自动提交就相当于 offsetManager.Commit() 操作，还是需要手动调用 MarkOffset。
*/

import (
	"fmt"
	"log"

	"sarama-wrapper/vars"

	"github.com/Shopify/sarama"
)

type OffsetManager struct {
	topic     string
	partition int32

	groupID string //从测试来看，producer和consumer必须指定相同的id

	kafkaClient sarama.Client

	nextOffset       int64                // 取得下一消息的偏移量作为本次消费的起点
	offsetManager    sarama.OffsetManager // 偏移量管理器
	partitionManager sarama.PartitionOffsetManager
}

func (m *OffsetManager) GetNextOffset() int64 {
	return m.nextOffset
}

//	创建OffsetManager
func NewOffsetManager(kafkaCli *sarama.Client, topic string, partition int32) (*OffsetManager, error) {
	var (
		err error
		a   string
	)
	man := &OffsetManager{
		topic:       topic,
		partition:   partition,
		groupID:     vars.DefaultstandaloneConsumerGroupID,
		kafkaClient: *kafkaCli,
	}

	// offsetManager 用于管理每个 consumerGroup 的 offset
	// 根据 groupID 来区分不同的 consumer，注意: 每次提交的 offset 信息也是和 groupID 关联的
	// 有个要注意的地方，如果想获取某个 partition 的 offset 位置，需要这个 offsetManager 的 groupId 和 consumer 的一致，否则拿到的 offset 是不正确的
	if man.offsetManager, err = sarama.NewOffsetManagerFromClient(man.groupID, man.kafkaClient); err != nil {
		log.Println("NewOffsetManagerFromClient err:", err)
		return nil, err
	}

	// 创建每个分区的 offset 也是分别管理的partitionOffsetManager，对应分区的偏移量管理器
	if man.partitionManager, err = man.offsetManager.ManagePartition(man.topic, man.partition); err != nil {
		log.Println("ManagePartition err:", err)
		return nil, err
	}

	// 根据 kafka 中记录的上次消费的 offset 开始 + 1 的位置接着消费
	man.nextOffset, a = man.partitionManager.NextOffset()
	fmt.Println(man.nextOffset, a)

	return man, nil
}

// 提交已消费位移
func (m *OffsetManager) Commit(offset int64) error {
	m.partitionManager.MarkOffset(offset+1, "modified metadata") // MarkOffset 更新最后消费的 offset
	return nil
}

// 关闭offset管理器
func (m *OffsetManager) Close() {
	// defer 在程序结束后在 commit 一次，防止自动提交间隔之间的信息被丢掉
	if m.offsetManager != nil {
		m.offsetManager.Commit()
	}
	if m.offsetManager != nil {
		m.offsetManager.Close()
	}

	if m.partitionManager != nil {
		m.partitionManager.Close()
	}

	return
}
