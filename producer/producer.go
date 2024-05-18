package main

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type MessagePayload struct {
	Message string `json:"message"`
	Journey string `json:"journey"`
}

func main() {
	// Kafka producer configuration
	config := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092", // Update this with your Kafka brokers
	}

	// Create Kafka producer
	producer, err := kafka.NewProducer(&config)
	if err != nil {
		log.Fatalf("Error creating producer: %v\n", err)
	}
	defer producer.Close()

	// Kafka topics
	topic1 := "test_topic1"
	topic2 := "test_topic2"

	// Create channels to receive delivery reports
	deliveryChan1 := make(chan kafka.Event)
	deliveryChan2 := make(chan kafka.Event)

	// Wait group to wait for all messages to be delivered
	wg := &sync.WaitGroup{}

	// Capture OS signals to gracefully shutdown
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	// Produce messages
	messageCount := 1000
	start := time.Now()

	for i := 0; i < messageCount; i++ {
		// Randomly select a topic
		var topic string
		if rand.Intn(2) == 0 {
			topic = topic1
		} else {
			topic = topic2
		}

		// Create JSON payload
		payload := MessagePayload{
			Message: fmt.Sprintf("Message %d", i),
			Journey: fmt.Sprintf("journey_%d", rand.Intn(2)+1),
		}

		// Marshal payload to JSON
		jsonPayload, err := json.Marshal(payload)
		if err != nil {
			log.Printf("Error marshalling JSON: %v\n", err)
			continue
		}

		// Produce message asynchronously
		err = producer.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          jsonPayload,
		}, chooseDeliveryChannel(deliveryChan1, deliveryChan2))

		if err != nil {
			log.Printf("Error producing message: %v\n", err)
		} else {
			wg.Add(1)
			go func() {
				defer wg.Done()
				e := <-chooseDeliveryChannel(deliveryChan1, deliveryChan2)
				m := e.(*kafka.Message)
				if m.TopicPartition.Error != nil {
					log.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
				} else {
					log.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
				}
			}()
		}
	}

	// Wait for all messages to be delivered
	wg.Wait()

	elapsed := time.Since(start)
	log.Printf("Produced %d messages in %s\n", messageCount, elapsed)

	// Wait for termination signal
	<-sigchan
	log.Println("Received termination signal. Shutting down...")
}

// chooseDeliveryChannel randomly selects one of the delivery channels.
func chooseDeliveryChannel(deliveryChan1, deliveryChan2 chan kafka.Event) chan kafka.Event {
	if rand.Intn(2) == 0 {
		return deliveryChan1
	}
	return deliveryChan2
}
