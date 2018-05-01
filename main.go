package main

import (
	"fmt"
	"time"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"strconv"
)

var (
	kafkaBroker = "localhost"
	kafkaTopic = "test"
	count = 0
	delayMs = 300
)

func main() {
	if os.Getenv("KAFKA_BROKER") != "" {
		kafkaBroker = os.Getenv("KAFKA_BROKER")
	}
	if os.Getenv("KAFKA_TOPIC") != "" {
		kafkaTopic = os.Getenv("KAFKA_TOPIC")
	}
	if c, err := strconv.Atoi(os.Getenv("MSG_COUNT")); err == nil {
		count = c
	}
	if d, err := strconv.Atoi(os.Getenv("MSG_DELAY_MS")); err == nil {
		delayMs = d
	}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		panic(err)
	}
	switch {
	case len(os.Args) == 2:
		c, err := strconv.Atoi(os.Args[1])
		if err != nil {
			fmt.Println("usage: kafka-producer [count]")
			os.Exit(1)
		} else {
			count = c
		}
	case len(os.Args) > 2:
		fmt.Println("usage: kafka-producer [count]")
		os.Exit(1)

	}

	// Delivery report handler for produced messages
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()

	// Produce messages to topic (asynchronously)
	cnt := 0
	if count == 0 {
		fmt.Printf("Start producer without msg limit. Delay between mgs '%dms'\n", delayMs)
	} else {
		fmt.Printf("Start producer with msg limit set to %d. Delay between mgs '%dms'\n", count, delayMs)
	}
	for {
		cnt++
		msg := fmt.Sprintf("Msg#%d: It is %s", cnt, time.Now().String())
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil)
		if count != 0 && cnt >= count {
			break
		}
		time.Sleep(time.Duration(delayMs)*time.Millisecond)
	}
	// Wait for message deliveries
	p.Flush(15 * 1000)
}
