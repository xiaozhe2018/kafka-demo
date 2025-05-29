package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

func main() {
	// 配置消费者
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 使用本地Kafka
	brokers := []string{"localhost:9092"}
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("创建消费者失败: %s", err)
	}
	defer consumer.Close()

	// 订阅主题
	topic := "kafka-demo" // 本地测试主题
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("获取主题分区失败: %s", err)
	}

	// 创建一个接收系统信号的通道
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 创建一个通道，用于关闭消费者
	done := make(chan bool)

	// 为每个分区创建一个消费者
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("创建分区消费者失败: %s", err)
		}

		// 在后台处理消息
		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					log.Printf("收到原始消息: topic=%s, partition=%d, offset=%d, key=%s",
						msg.Topic, msg.Partition, msg.Offset, string(msg.Key))

					// 尝试解析消息
					var message Message
					if err := json.Unmarshal(msg.Value, &message); err != nil {
						log.Printf("解析消息失败: %s, 原始内容: %s", err, string(msg.Value))
						continue
					}

					log.Printf("解析后消息: id=%s, type=%s, content=%s",
						message.ID, message.Type, message.Content)

				case err := <-pc.Errors():
					log.Printf("消费者错误: %s", err)

				case <-done:
					return
				}
			}
		}(pc)
	}

	log.Printf("\n[%s] ✅ Kafka消费示例服务启动成功", time.Now().Format("2006-01-02 15:04:05"))
	log.Println("------------------------------------")
	log.Printf("  订阅主题: %s", topic)
	log.Printf("  Kafka地址: %v", brokers)
	log.Println("------------------------------------")

	// 等待终止信号
	<-signals
	log.Println("接收到终止信号，停止消费者...")
	close(done)
} 