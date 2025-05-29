package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// 自定义分区器
type CustomPartitioner struct {
	partition int32
}

func NewCustomPartitioner(topic string) sarama.Partitioner {
	return &CustomPartitioner{}
}

func (p *CustomPartitioner) Partition(message *sarama.ProducerMessage, numPartitions int32) (int32, error) {
	// 从消息键中获取类型
	if key, ok := message.Key.(sarama.StringEncoder); ok {
		// 根据消息类型决定分区
		switch string(key) {
		case "info":
			return 0, nil
		case "warning":
			return 1 % numPartitions, nil
		case "error":
			return 2 % numPartitions, nil
		case "critical":
			return 3 % numPartitions, nil
		}
	}
	// 默认返回第一个分区
	return 0, nil
}

func (p *CustomPartitioner) RequiresConsistency() bool {
	return true
}

type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// 解析命令行参数
	var mode string
	flag.StringVar(&mode, "mode", "producer", "运行模式: producer 或 consumer")
	flag.Parse()

	// Kafka 配置
	brokers := []string{"localhost:9092"}
	topic := "kafka-demo-partition"

	// 创建一个接收系统信号的通道
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	switch mode {
	case "producer":
		runProducer(brokers, topic, signals)
	case "consumer":
		runConsumer(brokers, topic, signals)
	default:
		log.Fatalf("未知模式: %s. 使用 -mode=producer 或 -mode=consumer", mode)
	}
}

func runProducer(brokers []string, topic string, signals chan os.Signal) {
	// 配置生产者
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// 注册自定义分区器
	config.Producer.Partitioner = NewCustomPartitioner

	// 连接到Kafka
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("创建生产者失败: %s", err)
	}
	defer producer.Close()

	// 创建一个通道，用于关闭生产者
	done := make(chan bool)

	// 在后台发送消息
	go func() {
		// 消息类型列表
		types := []string{"info", "warning", "error", "critical"}
		counter := 0

		for {
			select {
			case <-done:
				return
			default:
				counter++
				// 创建消息
				msgType := types[counter%len(types)]
				msg := Message{
					ID:        strconv.Itoa(counter),
					Content:   fmt.Sprintf("这是%s类型的测试消息 #%d", msgType, counter),
					Type:      msgType,
					Timestamp: time.Now(),
				}

				// 序列化消息
				msgBytes, err := json.Marshal(msg)
				if err != nil {
					log.Printf("序列化消息失败: %s", err)
					continue
				}

				// 发送消息
				partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
					Topic: topic,
					Key:   sarama.StringEncoder(msg.Type), // 使用消息类型作为key
					Value: sarama.ByteEncoder(msgBytes),
				})

				if err != nil {
					log.Printf("发送消息失败: %s", err)
				} else {
					log.Printf("消息发送成功: topic=%s, partition=%d, offset=%d, id=%s, type=%s",
						topic, partition, offset, msg.ID, msg.Type)
				}

				// 每秒发送一条消息
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// 等待终止信号
	<-signals
	log.Println("接收到终止信号，停止生产者...")
	done <- true
}

func runConsumer(brokers []string, topic string, signals chan os.Signal) {
	// 配置消费者
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	// 连接到Kafka
	consumer, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		log.Fatalf("创建消费者失败: %s", err)
	}
	defer consumer.Close()

	// 获取主题的分区列表
	partitionList, err := consumer.Partitions(topic)
	if err != nil {
		log.Fatalf("获取主题分区失败: %s", err)
	}

	// 创建一个通道，用于关闭消费者
	done := make(chan bool)

	// 为每个分区创建一个消费者
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(topic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("创建分区消费者失败: %s", err)
		}

		// 在后台处理消息
		go func(pc sarama.PartitionConsumer, partition int32) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					var message Message
					if err := json.Unmarshal(msg.Value, &message); err != nil {
						log.Printf("解析消息失败: %s", err)
						continue
					}

					log.Printf("分区 %d 收到消息: topic=%s, offset=%d, id=%s, type=%s, content=%s",
						partition, msg.Topic, msg.Offset, message.ID, message.Type, message.Content)

				case err := <-pc.Errors():
					log.Printf("消费者错误: %s", err)

				case <-done:
					return
				}
			}
		}(pc, partition)
	}

	// 等待终止信号
	<-signals
	log.Println("接收到终止信号，停止消费者...")
	close(done)
} 