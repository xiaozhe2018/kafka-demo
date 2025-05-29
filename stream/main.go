package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// 消息结构
type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
	Count     int       `json:"count,omitempty"`
}

// 聚合结果
type AggregateResult struct {
	Type  string `json:"type"`
	Count int    `json:"count"`
}

func main() {
	// Kafka 配置
	brokers := []string{"localhost:9092"}
	sourceTopic := "kafka-stream-source"
	sinkTopic := "kafka-stream-sink"

	// 创建一个接收系统信号的通道
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 等待组，用于优雅关闭
	var wg sync.WaitGroup

	// 创建生产者（数据源）
	wg.Add(1)
	go func() {
		defer wg.Done()
		runProducer(brokers, sourceTopic, signals)
	}()

	// 创建处理器（流处理）
	wg.Add(1)
	go func() {
		defer wg.Done()
		runProcessor(brokers, sourceTopic, sinkTopic, signals)
	}()

	// 创建消费者（数据接收）
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(brokers, sinkTopic, signals)
	}()

	// 等待终止信号
	<-signals
	log.Println("接收到终止信号，等待所有组件关闭...")
	wg.Wait()
}

// 生产者：生成原始消息
func runProducer(brokers []string, topic string, signals chan os.Signal) {
	// 配置生产者
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 5
	config.Producer.Return.Successes = true

	// 连接到Kafka
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("创建生产者失败: %s", err)
	}
	defer producer.Close()

	// 创建一个通道，用于关闭生产者
	done := make(chan bool, 1)

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
					Content:   fmt.Sprintf("这是%s类型的原始消息 #%d", msgType, counter),
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
					log.Printf("发送原始消息失败: %s", err)
				} else {
					log.Printf("原始消息发送成功: topic=%s, partition=%d, offset=%d, id=%s, type=%s",
						topic, partition, offset, msg.ID, msg.Type)
				}

				// 每秒发送一条消息
				time.Sleep(1 * time.Second)
			}
		}
	}()

	// 等待终止信号的副本
	signalCopy := make(chan os.Signal, 1)
	go func() {
		<-signals
		signalCopy <- syscall.SIGTERM
	}()

	// 等待终止信号
	<-signalCopy
	log.Println("接收到终止信号，停止生产者...")
	done <- true
}

// 处理器：处理消息并将结果写入另一个主题
func runProcessor(brokers []string, sourceTopic, sinkTopic string, signals chan os.Signal) {
	// 配置消费者
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Return.Errors = true

	// 配置生产者
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Retry.Max = 5
	producerConfig.Producer.Return.Successes = true

	// 连接到Kafka (消费者)
	consumer, err := sarama.NewConsumer(brokers, consumerConfig)
	if err != nil {
		log.Fatalf("创建消费者失败: %s", err)
	}
	defer consumer.Close()

	// 连接到Kafka (生产者)
	producer, err := sarama.NewSyncProducer(brokers, producerConfig)
	if err != nil {
		log.Fatalf("创建生产者失败: %s", err)
	}
	defer producer.Close()

	// 获取源主题的分区列表
	partitionList, err := consumer.Partitions(sourceTopic)
	if err != nil {
		log.Fatalf("获取主题分区失败: %s", err)
	}

	// 创建一个通道，用于关闭处理器
	done := make(chan bool, 1)

	// 消息类型计数器
	typeCounts := make(map[string]int)
	var countMutex sync.Mutex

	// 为每个分区创建一个消费者
	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(sourceTopic, partition, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("创建分区消费者失败: %s", err)
		}

		// 在后台处理消息
		go func(pc sarama.PartitionConsumer) {
			defer pc.Close()
			for {
				select {
				case msg := <-pc.Messages():
					// 解析消息
					var message Message
					if err := json.Unmarshal(msg.Value, &message); err != nil {
						log.Printf("解析消息失败: %s", err)
						continue
					}

					// 计数聚合
					countMutex.Lock()
					typeCounts[message.Type]++
					count := typeCounts[message.Type]
					countMutex.Unlock()

					// 创建聚合消息
					aggregateMsg := Message{
						ID:        message.ID,
						Content:   fmt.Sprintf("处理后的%s类型消息", message.Type),
						Type:      message.Type,
						Timestamp: time.Now(),
						Count:     count,
					}

					// 序列化消息
					msgBytes, err := json.Marshal(aggregateMsg)
					if err != nil {
						log.Printf("序列化聚合消息失败: %s", err)
						continue
					}

					// 发送聚合消息
					partition, offset, err := producer.SendMessage(&sarama.ProducerMessage{
						Topic: sinkTopic,
						Key:   sarama.StringEncoder(aggregateMsg.Type),
						Value: sarama.ByteEncoder(msgBytes),
					})

					if err != nil {
						log.Printf("发送聚合消息失败: %s", err)
					} else {
						log.Printf("聚合消息发送成功: topic=%s, partition=%d, offset=%d, type=%s, count=%d",
							sinkTopic, partition, offset, aggregateMsg.Type, aggregateMsg.Count)
					}

				case err := <-pc.Errors():
					log.Printf("处理器错误: %s", err)

				case <-done:
					return
				}
			}
		}(pc)
	}

	// 等待终止信号的副本
	signalCopy := make(chan os.Signal, 1)
	go func() {
		<-signals
		signalCopy <- syscall.SIGTERM
	}()

	// 等待终止信号
	<-signalCopy
	log.Println("接收到终止信号，停止处理器...")
	close(done)
}

// 消费者：读取处理后的消息
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
	done := make(chan bool, 1)

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
					var message Message
					if err := json.Unmarshal(msg.Value, &message); err != nil {
						log.Printf("解析消息失败: %s", err)
						continue
					}

					log.Printf("处理后消息接收成功: topic=%s, partition=%d, offset=%d, id=%s, type=%s, count=%d",
						msg.Topic, msg.Partition, msg.Offset, message.ID, message.Type, message.Count)

				case err := <-pc.Errors():
					log.Printf("消费者错误: %s", err)

				case <-done:
					return
				}
			}
		}(pc)
	}

	// 等待终止信号的副本
	signalCopy := make(chan os.Signal, 1)
	go func() {
		<-signals
		signalCopy <- syscall.SIGTERM
	}()

	// 等待终止信号
	<-signalCopy
	log.Println("接收到终止信号，停止消费者...")
	close(done)
} 