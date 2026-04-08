package output

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

// KafkaWriter publishes decoded messages to a Kafka topic.
type KafkaWriter struct {
	producer *kafka.Producer
	topic    string
}

// NewKafkaWriter creates a producer and starts draining delivery reports in
// the background.
func NewKafkaWriter(brokers, topic string) (*KafkaWriter, error) {
	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": brokers,
	})
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	w := &KafkaWriter{producer: p, topic: topic}
	go w.drainDeliveryReports()
	return w, nil
}

// Write enqueues jsonBytes for delivery to the configured topic.
func (w *KafkaWriter) Write(jsonBytes []byte) error {
	return w.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &w.topic, Partition: kafka.PartitionAny},
		Value:          jsonBytes,
	}, nil)
}

// Close flushes pending messages and shuts down the producer.
func (w *KafkaWriter) Close() error {
	w.producer.Flush(5000)
	w.producer.Close()
	return nil
}

func (w *KafkaWriter) drainDeliveryReports() {
	for e := range w.producer.Events() {
		if msg, ok := e.(*kafka.Message); ok {
			if msg.TopicPartition.Error != nil {
				log.Printf("Delivery failed: %v", msg.TopicPartition.Error)
			}
		}
	}
}
