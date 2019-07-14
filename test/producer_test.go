package test

import (
	"fmt"
	"testing"
	"time"

	"github.com/kafka-async/promise"
	"github.com/stretchr/testify/assert"
)

type TestMessage struct {
	Key   string
	Value string
}

func TestProducer(t *testing.T) {
	topic := "test"
	message := promise.Message{Topic: &topic, Value: TestMessage{Key: "Some key", Value: "Some Value"}}
	config := promise.KafkaConfig{BoostrapServers: "localhost"}
	client := promise.CreateKafkaClient(config)

	client.PublishMessage(message).Then(func(obj interface{}) (interface{}, error) {
		assert.NotEmpty(t, obj, "The message shouldn't be nil")
		return nil, nil
	}).Catch(func(err error) {
		t.Fail()
	})
	time.Sleep(5 * time.Second)
	fmt.Println("Done!")

}
