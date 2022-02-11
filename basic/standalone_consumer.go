package main

import (
	"log"
	"time"

	"sarama-wrapper/offset_manager"
	"sarama-wrapper/vars"

	"github.com/Shopify/sarama"
)

func main() {
	var (
		topic string = "test"
	)
	config := sarama.NewConfig()
	// 配置开启自动提交 offset，这样 samara 库会定时帮我们把最新的 offset 信息提交给 kafka
	config.Consumer.Offsets.AutoCommit.Enable = true              // 开启自动 commit offset
	config.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second // 自动 commit 时间间隔
	client, err := sarama.NewClient([]string{vars.HOST}, config)
	if err != nil {
		log.Fatal("NewClient err:", err)
	}
	defer client.Close()

	//创建consumer
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Println("NewConsumerFromClient err:", err)
		return
	}

	// 创建offset管理器
	offsetman, err := offset_manager.NewOffsetManager(&client, topic, 0)
	if err != nil {
		log.Println("NewOffsetManager err:", err)
		return
	}

	defer offsetman.Close()

	pc, err := consumer.ConsumePartition(topic, 0, offsetman.GetNextOffset())
	if err != nil {
		log.Println("ConsumePartition err:", err)
		return
	}

	defer pc.Close()

	for message := range pc.Messages() {
		value := string(message.Value)
		// 拿到下一个 offset
		log.Printf("[Consumer] partitionid: %d; offset:%d, value: %s\n", message.Partition, message.Offset, value)
		// 每次消费后都更新一次 offset, 这里更新的只是程序内存中的值，需要 commit 之后才能提交到 kafka
		// 提交 offset，默认提交到本地缓存，每秒钟往 broker 提交一次（可以设置）
		offsetman.Commit(message.Offset) // MarkOffset 更新最后消费的 offset
	}
}
