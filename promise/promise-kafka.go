package promise

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// KafkaConfig is a config for using kafka
type KafkaConfig struct {
	BoostrapServers string
}

// KafkaClient is a client that can consumes and produces messages
type KafkaClient struct {
	producer map[string]*kafka.Producer
	consumer map[string]*kafka.Consumer
	config   KafkaConfig
}

// Message is a Kafka messages to be send to the producer
type Message struct {
	Topic   *string
	Headers map[string]string
	Value   interface{}
}

// CreateKafkaClient creates a kaka client from config
func CreateKafkaClient(config KafkaConfig) *KafkaClient {
	return &KafkaClient{config: config}
}

func (client *KafkaClient) ensureProducer() {
	if client.producer == nil {
		client.producer = make(map[string]*kafka.Producer)
	}
	if len(client.producer) > 0 {
		for k, producer := range client.producer {
			if producer == nil {
				p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": client.config.BoostrapServers})
				if err == nil {
					client.producer[k] = p
				} else {
					panic("Couldn't create a producer for " + k)
				}
			}
		}
	}
}

func (client *KafkaClient) addProducer(topic string) {
	fmt.Println(topic)
	if _, exists := client.producer[topic]; exists {
		return
	}
	client.ensureProducer()
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": client.config.BoostrapServers})
	if err == nil {
		client.producer[topic] = p
	} else {
		fmt.Println(err)
		panic("Couldn't create a producer for " + topic)
	}
}

// PublishMessage publishes messages to a kafka stream then returns a promise
func (client *KafkaClient) PublishMessage(message Message) Promise {
	return FromFunc(func() (interface{}, error) {
		client.addProducer(*message.Topic)
		payload, err := stringify(message.Value)
		if err != nil {
			return nil, err
		}
		return Message{Topic: message.Topic, Headers: message.Headers, Value: payload}, nil
	}).Then(func(msg interface{}) (interface{}, error) {
		topic := *msg.(Message).Topic
		if producer, ok := client.producer[topic]; ok {
			deliveryChan := make(chan kafka.Event)
			err := producer.Produce(createMessage(msg.(Message)), deliveryChan)
			if err != nil {
				return nil, err
			}
			return deliveryChan, nil
		}
		return nil, errors.New("Topic doesn't exist " + topic)
	}).Then(func(msg interface{}) (interface{}, error) {
		deliveryChan := msg.(chan kafka.Event)
		e := <-deliveryChan
		m := e.(*kafka.Message)
		close(deliveryChan)
		if m.TopicPartition.Error != nil {
			return nil, fmt.Errorf("Delivery failed: %v\n", m.TopicPartition.Error)
		}
		log.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
		return m, nil
	})
}

func stringify(value interface{}) ([]byte, error) {
	switch v := value.(type) {
	case string:
		return []byte(v), nil
	default:
		return json.Marshal(v)
	}
}

func createMessage(msg Message) *kafka.Message {
	// headers map[string]string, value []byte, topic *string
	if msg.Headers == nil {
		return &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: msg.Topic, Partition: kafka.PartitionAny}, Value: msg.Value.([]byte)}
	}
	lis := make([]kafka.Header, 0)
	for name, value := range msg.Headers {
		lis = append(lis, kafka.Header{Key: name, Value: []byte(value)})
	}
	return &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: msg.Topic, Partition: kafka.PartitionAny}, Value: msg.Value.([]byte), Headers: lis}
}
