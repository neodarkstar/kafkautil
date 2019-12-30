package kafkautil

import (
	"strings"
	"testing"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

func TestLoadConfig(t *testing.T) {
	config, err := UnmarshalConfig(".", "kafka-config.yml")

	if err != nil {
		t.Error("Got an Error")
	}

	if len(config.Hosts) != 1 {
		t.Error("Invalid Hosts")
	}

	if config.ReplicationFactor != 3 {
		t.Error("Invalid Replication Factor")
	}

	if len(config.Topics) != 3 {
		t.Error("Invalid Topics")
	}

	if config.Topics[0].Family != "metadata" {
		t.Error("Invalid Topic Family")
	}

	if config.Topics[0].Partitions != 1 {
		t.Error("Invalid Partition Count")
	}

	if config.Group != "unit_test1" {
		t.Error("Invalid Group")
	}
}

func connect() *ACXKafkaClient {
	config := UnmarshalConfig(".", "kafka-config.yml")

	client, _ := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": config.Hosts,
	})

	return &ACXKafkaClient{AdminClient: client}
}

func TestCreateTopics(t *testing.T) {
	_, err := CreateTopics(config)
	if err != nil {
		t.Error("Got an Error", err)
	}
}

func TestCreateTopicsError(t *testing.T) {
	results, err := CreateTopics(config)
	if results != nil {
		t.Error("Got an Error", err)
	}
}

func TestDeleteTopics(t *testing.T) {
	_, err := DeleteTopics(config)
	if err != nil {
		t.Error("Got an Error", err)
	}
}

func TestDeleteTopicsError(t *testing.T) {
	_, err := DeleteTopics(config)
	if err == nil {
		t.Error("Got an Error", err)
	}
}

func TestValidateTopics(t *testing.T) {
	CreateTopics(config)

	topics := parseConfig(config)
	flag, _ := validateTopics(&topics, strings.Join(config.Hosts, ","))

	if flag == false {
		t.Error("Flag is false")
	}

	DeleteTopics(config)
}
