package input

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaReader reads raw goreplay messages from a Kafka topic.
type KafkaReader struct {
	consumer *kafka.Consumer
}

// KafkaConfig holds the parameters for creating a KafkaReader.
type KafkaConfig struct {
	Brokers       string
	Topic         string
	ConsumerGroup string
	// StartOffset, if >= 0, pins reading to this partition offset bypassing
	// any committed group offset. Use kafka.OffsetBeginning (-2) for "from start".
	StartOffset int64
	// Debug disables auto-commit so group offsets are not advanced.
	Debug bool
}

// NewKafkaReader creates a Kafka consumer and positions it according to cfg.
func NewKafkaReader(cfg KafkaConfig) (*KafkaReader, error) {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  cfg.Brokers,
		"group.id":           cfg.ConsumerGroup,
		"auto.offset.reset":  "earliest",
		"enable.auto.commit": !cfg.Debug,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}

	if cfg.StartOffset >= 0 || cfg.Debug {
		offset := kafka.OffsetBeginning
		if cfg.StartOffset >= 0 {
			offset = kafka.Offset(cfg.StartOffset)
		}
		if err := c.Assign([]kafka.TopicPartition{{
			Topic:     &cfg.Topic,
			Partition: 0,
			Offset:    offset,
		}}); err != nil {
			c.Close()
			return nil, fmt.Errorf("assign partition: %w", err)
		}
	} else {
		if err := c.SubscribeTopics([]string{cfg.Topic}, nil); err != nil {
			c.Close()
			return nil, fmt.Errorf("subscribe to %s: %w", cfg.Topic, err)
		}
	}

	return &KafkaReader{consumer: c}, nil
}

// Read blocks up to timeout for the next Kafka message.
func (r *KafkaReader) Read(timeout time.Duration) (*Message, error) {
	msg, err := r.consumer.ReadMessage(timeout)
	if err != nil {
		if kerr, ok := err.(kafka.Error); ok && kerr.Code() == kafka.ErrTimedOut {
			return nil, ErrTimeout{}
		}
		return nil, err
	}
	return &Message{Value: msg.Value}, nil
}

// Close shuts down the Kafka consumer.
func (r *KafkaReader) Close() error {
	return r.consumer.Close()
}
