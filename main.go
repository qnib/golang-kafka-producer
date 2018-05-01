package main

import (
	"fmt"
	"time"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/Shopify/sarama"
	"os"
	"strconv"
	"strings"
)

var (
	kafkaBroker = "localhost"
	kafkaPort = "9092"
	kafkaTopic = "test"
	count = -1
	delayMs = 300
)

func createTopic(brokerAdd, topic string, numPartitions, numReplicas int) (bool) {
	broker := sarama.NewBroker(brokerAdd)
	defer broker.Close()
	err := broker.Open(nil)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	request := sarama.MetadataRequest{}
	response, err := broker.GetMetadata(&request)
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	create := true
	for _, reTopic := range response.Topics {
		if  topic == reTopic.Name {
			create = false
		}
	}
	if create {
		req  := &sarama.CreateTopicsRequest{}
		req.TopicDetails = make(map[string]*sarama.TopicDetail)
		req.TopicDetails[topic] = &sarama.TopicDetail{
			NumPartitions: int32(numPartitions),
			ReplicationFactor: int16(numReplicas)}
		_, err := broker.CreateTopics(req)
		if err != nil {
			fmt.Println(err.Error())
			return false
		}
	}
	return true
}

func main() {
	if os.Getenv("KAFKA_BROKER") != "" {
		kafkaBroker = os.Getenv("KAFKA_BROKER")
	}
	if os.Getenv("KAFKA_PORT") != "" {
		kafkaPort = os.Getenv("KAFKA_PORT")
	}
	if os.Getenv("KAFKA_CREATE_TOPICS") != "" {
		for _, topic := range strings.Split(os.Getenv("KAFKA_CREATE_TOPICS"), ",") {
			s := strings.Split(topic, ":")
			if len(s) != 3 {
				fmt.Printf("Format for KAFKA_CREATE_TOPICS must be 'name:partitions:replicas' separated by comma: %s\n", topic)
				os.Exit(1)
			}
			part, err := strconv.Atoi(s[1])
			if err != nil {
				fmt.Printf("Format for KAFKA_CREATE_TOPICS must be 'name:partitions:replicas' separated by comma: %s\n", topic)
				os.Exit(1)
			}
			repl, err := strconv.Atoi(s[2])
			if err != nil {
				fmt.Printf("Format for KAFKA_CREATE_TOPICS must be 'name:partitions:replicas' separated by comma: %s\n", topic)
				os.Exit(1)
			}
			success := createTopic(fmt.Sprintf("%s:%s", kafkaBroker, kafkaPort), s[0], part, repl)
			if success {
				fmt.Printf("OK  -> createTopic(%s:%s, %s, %d, %d)\n", kafkaBroker, kafkaPort, s[0], part, repl)
			} else {
				fmt.Printf("FAIL-> createTopic(%s:%s, %s, %d, %d)\n", kafkaBroker, kafkaPort, s[0], part, repl)
			}
		}
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
	if count == -1 {
		fmt.Printf("Start producer without msg limit. Delay between mgs '%dms'\n", delayMs)
	} else {
		fmt.Printf("Start producer with msg limit set to %d. Delay between mgs '%dms'\n", count, delayMs)
	}
	for {
		if count == 0 {
			fmt.Println("Count==0; exit producer")
			break
		}
		cnt++
		msg := fmt.Sprintf("Msg#%d: It is %s", cnt, time.Now().String())
		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
			Value:          []byte(msg),
		}, nil)
		if cnt >= count {
			break
		}
		time.Sleep(time.Duration(delayMs)*time.Millisecond)
	}
	// Wait for message deliveries
	p.Flush(15 * 1000)
}
