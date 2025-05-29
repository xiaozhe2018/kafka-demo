package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

// 消费者组处理器
type ConsumerGroupHandler struct {
	ready chan bool
}

type Message struct {
	ID        string `json:"id"`
	Content   string `json:"content"`
	Type      string `json:"type"`
	Timestamp string `json:"timestamp"`
}

// Setup 是在消费者会话开始时调用
func (h *ConsumerGroupHandler) Setup(sarama.ConsumerGroupSession) error {
	close(h.ready)
	return nil
}

// Cleanup 是在消费者会话结束时调用
func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim 负责消费消息
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		log.Printf("收到原始消息: topic=%s, partition=%d, offset=%d, key=%s",
			msg.Topic, msg.Partition, msg.Offset, string(msg.Key))

		// 尝试解析消息
		var message Message
		if err := json.Unmarshal(msg.Value, &message); err != nil {
			log.Printf("解析消息失败: %s, 原始内容: %s", err, string(msg.Value))
			continue
		}

		log.Printf("[消费者组] 解析后消息: id=%s, type=%s, content=%s",
			message.ID, message.Type, message.Content)

		// 标记消息已处理
		session.MarkMessage(msg, "")
	}
	return nil
}

func main() {
	// 配置消费者组
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin

	// 连接到实际Kafka集群
	// brokers := []string{"192.168.10.35:9092", "192.168.10.36:9092", "192.168.10.37:9092"}
	brokers := []string{"localhost:9092"}
	topic := "test-topic" // 使用通用测试主题
	group := "demo-consumer-group"

	// 创建一个接收系统信号的通道
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	// 创建上下文
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 创建消费者组
	client, err := sarama.NewConsumerGroup(brokers, group, config)
	if err != nil {
		log.Fatalf("创建消费者组失败: %s", err)
	}
	defer client.Close()

	// 跟踪消费者组错误
	go func() {
		for err := range client.Errors() {
			log.Printf("消费者组错误: %s", err)
		}
	}()

	// 创建消费者组处理器
	handler := &ConsumerGroupHandler{
		ready: make(chan bool),
	}

	// 等待组消费完成的WaitGroup
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			// 消费循环，如果消费者组重新平衡，会重新进入这个循环
			if err := client.Consume(ctx, []string{topic}, handler); err != nil {
				log.Printf("消费错误: %s", err)
			}
			// 检查上下文是否已取消
			if ctx.Err() != nil {
				return
			}
			handler.ready = make(chan bool)
		}
	}()

	// 等待消费者准备就绪
	<-handler.ready
	
	log.Printf("\n[%s] ✅ Kafka消费者组示例服务启动成功", time.Now().Format("2006-01-02 15:04:05"))
	log.Println("------------------------------------")
	log.Printf("  运行模式: 消费者组")
	log.Printf("  消费者组: %s", group)
	log.Printf("  订阅主题: %s", topic)
	log.Printf("  Kafka地址: %v", brokers)
	log.Println("------------------------------------")

	// 等待终止信号
	<-signals
	log.Println("接收到终止信号，停止消费者组...")
	cancel()
	wg.Wait()
} 