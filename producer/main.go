package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

type Message struct {
	ID        string    `json:"id"`
	Content   string    `json:"content"`
	Type      string    `json:"type"`
	Timestamp time.Time `json:"timestamp"`
}

func main() {
	// 配置生产者
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll      // 等待所有副本确认
	config.Producer.Retry.Max = 5                         // 重试最大次数
	config.Producer.Return.Successes = true               // 成功发送的消息将在success channel返回

	// 使用本地Kafka
	brokers := []string{"localhost:9092"}
	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("创建生产者失败: %s", err)
	}
	defer producer.Close()

	// 创建一个接收系统信号的通道
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 创建一个用于发送消息的通道
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
				msg := Message{
					ID:        strconv.Itoa(counter),
					Content:   fmt.Sprintf("这是测试消息 #%d", counter),
					Type:      types[counter%len(types)],
					Timestamp: time.Now(),
				}

				// 序列化消息
				msgBytes, err := json.Marshal(msg)
				if err != nil {
					log.Printf("序列化消息失败: %s", err)
					continue
				}

				// 发送消息
				topic := "kafka-demo" // 本地测试主题
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

	log.Printf("\n[%s] ✅ Kafka生产者示例服务启动成功", time.Now().Format("2006-01-02 15:04:05"))
	log.Println("------------------------------------")
	log.Printf("  发送主题: kafka-demo")
	log.Printf("  Kafka地址: %v", brokers)
	log.Println("------------------------------------")

	// 等待终止信号
	<-signals
	log.Println("接收到终止信号，停止生产者...")
	done <- true
} 