# Kafka 示例项目

这个项目包含了几个 Kafka 的示例程序，展示了不同的 Kafka 使用场景。

## 目录结构

```
kafka-demo/
├── producer/           # 基本生产者示例
├── consumer/           # 基本消费者示例
├── consumer-group/     # 消费者组示例
├── partition/          # 自定义分区策略示例
├── stream/             # 简单流处理示例
└── README.md           # 项目说明
```

## 依赖安装

项目使用了 IBM/sarama 库（原 Shopify/sarama）：

```bash
go get github.com/IBM/sarama
```

## Kafka 连接配置

示例代码默认连接到生产环境的 Kafka 集群：

```go
brokers := []string{"192.168.10.35:9092", "192.168.10.36:9092", "192.168.10.37:9092"}
```

如果需要连接到本地 Kafka，可以修改为：

```go
brokers := []string{"localhost:9092"}
```

并使用以下 Docker 命令启动本地 Kafka：

```bash
docker run -p 9092:9092 -e ALLOW_PLAINTEXT_LISTENER=yes bitnami/kafka:latest
```

## 运行示例

### 基本生产者/消费者

1. 在一个终端窗口启动消费者：

```bash
cd consumer
go run main.go
```

2. 在另一个终端窗口启动生产者：

```bash
cd producer
go run main.go
```

### 消费者组

```bash
cd consumer-group
go run main.go
```

可以启动多个实例来查看消费者组的负载均衡效果。

### 自定义分区策略

1. 先启动消费者模式：

```bash
cd partition
go run main.go -mode=consumer
```

2. 再启动生产者模式：

```bash
cd partition
go run main.go -mode=producer
```

### 流处理示例

```bash
cd stream
go run main.go
```

这个示例会启动一个完整的流处理管道，包含：
- 生产者（产生原始数据）
- 处理器（处理数据并计数）
- 消费者（显示处理结果）

## 注意事项

- 所有示例均可使用 Ctrl+C 优雅退出
- 生产者和消费者示例已配置为连接到实际生产环境 Kafka 集群
- 示例默认使用 `kafka_demo` 主题，与现有服务保持一致 
