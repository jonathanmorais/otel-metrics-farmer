package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
)

type MessagePayload struct {
	Message string `json:"message"`
	Journey string `json:"journey"`
}

var serviceName = semconv.ServiceNameKey.String("test-service")

func initConn() (*grpc.ClientConn, error) {
	// It connects the OpenTelemetry Collector through local gRPC connection.
	// You may replace `localhost:4317` with your endpoint.
	conn, err := grpc.Dial("localhost:4317",
		// Note the use of insecure transport here. TLS is recommended in production.
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection to collector: %w", err)
	}

	return conn, err
}

// Initializes an OTLP exporter, and configures the corresponding meter provider.
func initMeterProvider(ctx context.Context, res *resource.Resource, conn *grpc.ClientConn) (func(context.Context) error, error) {
	metricExporter, err := otlpmetricgrpc.New(ctx, otlpmetricgrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics exporter: %w", err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(meterProvider)

	return meterProvider.Shutdown, nil
}

func main() {
	log.Printf("Waiting for connection...")

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt)
	defer cancel()

	conn, err := initConn()
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	res, err := resource.New(ctx,
		resource.WithAttributes(
			// The service name used to display traces in backends
			serviceName,
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	shutdownMeterProvider, err := initMeterProvider(ctx, res, conn)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := shutdownMeterProvider(ctx); err != nil {
			log.Fatalf("failed to shutdown MeterProvider: %s", err)
		}
	}()

	meter := otel.Meter("test-meter")

	runCount1, err := meter.Int64Counter("run_journey_1", metric.WithDescription("The number of times the iteration ran for journey_1"))
	if err != nil {
		log.Fatal(err)
	}

	runCount2, err := meter.Int64Counter("run_journey_2", metric.WithDescription("The number of times the iteration ran for journey_2"))
	if err != nil {
		log.Fatal(err)
	}

	// Kafka consumer configuration
	config := kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092", // Update this with your Kafka brokers
		"group.id":          "my-consumer-group",
		"auto.offset.reset": "earliest",
	}

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return
	}
	defer consumer.Close()

	// Subscribe to topics
	topics := []string{"test_topic"} // Change this to your topic
	err = consumer.SubscribeTopics(topics, nil)
	if err != nil {
		fmt.Printf("Error subscribing to topics: %v\n", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}

			switch e := ev.(type) {
			case *kafka.Message:
				var payload MessagePayload
				err := json.Unmarshal(e.Value, &payload)
				if err != nil {
					fmt.Printf("Error decoding JSON: %v\n", err)
					continue
				}

				switch payload.Journey {
				case "journey_1":
					runCount1.Add(ctx, 1)
				case "journey_2":
					runCount2.Add(ctx, 1)
				default:
					log.Printf("Unknown journey value: %s", payload.Journey)
				}

				// Process the message here...
				fmt.Printf("Received message: %s, Journey: %s\n", payload.Message, payload.Journey)
			case kafka.Error:
				fmt.Printf("Error: %v\n", e)
				return
			}
		}
	}
}
