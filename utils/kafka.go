package utils

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

func PublishMessage(topic, key string, message []byte) error {
	// The job of the key here is to enforce queuing mechanism.
	// Same keys will be routed to the same partition.
	// If you dont care about the order drop the key.

	host := os.Getenv("KAFKA_HOST")

	w := &kafka.Writer{
		Addr:                   kafka.TCP(host),
		Topic:                  topic,
		AllowAutoTopicCreation: true,
	}

	messages := []kafka.Message{
		{
			Key:   []byte(key),
			Value: message,
		},
	}

	var err error
	const retries = 3
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// attempt to create topic prior to publishing the message
		err = w.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
		break
	}

	if err := w.Close(); err != nil {
		return fmt.Errorf("failed to close writer:", err)
	}

	return nil
}

func ConsumeMessage(topic string, wg *sync.WaitGroup) {

	defer wg.Done()

	host := os.Getenv("KAFKA_HOST")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{host},
		Topic:   topic,
		GroupID: "my-consumer-group",
	})

	defer reader.Close()

	for {
		msg, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Println(err)
			continue
		}

		fmt.Println("Received:", string(msg.Value))
	}
}
