package kafkautil

import (
	"strings"
	"testing"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestLoadConfig(t *testing.T) {
	config := UnmarshalConfig(".", "kafka-config.yml")

	if len(config.Hosts) != 1 {
		t.Error("Invalid Hosts")
	}

	if len(config.Topics) != 3 {
		t.Error("Invalid Topics")
	}

	if config.Topics[0].NumPartitions != 1 {
		t.Error("Invalid Partition Count")
	}

	if config.Group != "unit_test1" {
		t.Error("Invalid Group")
	}
}

var config *KafkaConfig = UnmarshalConfig(".", "kafka-config.yml")

func connect(hosts string) *ACXKafkaClient {
	client, _ := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
	})

	return &ACXKafkaClient{AdminClient: client}
}

var client *ACXKafkaClient = connect(strings.Join(config.Hosts, ","))

func TestCreateTopics(t *testing.T) {
	client.CreateTopics(config.Topics)
}

func TestCreateTopicsExisting(t *testing.T) {
	result := client.CreateTopics(config.Topics)

	if len(result) != 0 {
		t.Error("Invalid Error")
	}
}

func TesValidateTopicsIncorrectTopicName(t *testing.T) {
	results, success := client.ValidateTopics([]kafka.TopicSpecification{kafka.TopicSpecification{
		Topic:             "monkeyDangus",
		NumPartitions:     1,
		ReplicationFactor: 3,
	}})

	if success == true {
		t.Error(results)
	}
}

func TestValidateTopicsIncorrectPartitionCount(t *testing.T) {
	results, success := client.ValidateTopics([]kafka.TopicSpecification{kafka.TopicSpecification{
		Topic:             config.Topics[0].Topic,
		NumPartitions:     config.Topics[0].NumPartitions + 1,
		ReplicationFactor: config.Topics[0].ReplicationFactor,
	}})

	if success == true {
		t.Error(results)
	}
}

func TestDeleteTopics(t *testing.T) {
	client.DeleteTopics(config.Topics)
}

func TestCreateTopicsError(t *testing.T) {
	client.CreateTopics(config.Topics)
}

func TestDeleteTopicsError(t *testing.T) {
	client.DeleteTopics(config.Topics)
}
