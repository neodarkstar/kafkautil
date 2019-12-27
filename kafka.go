package kafkautil

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// IsValid checks for the topic is found and the partition counts match
func IsValid(topic *kafka.TopicSpecification, hosts string) (*ACXTopicValidationResult, *ACXTopicValidationError) {
	client, _ := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
	})

	meta, _ := client.GetMetadata(nil, true, 2000)

	for _, t := range meta.Topics {
		if t.Topic == topic.Topic && len(t.Partitions) == topic.NumPartitions {
			client.Close()
			return &ACXTopicValidationResult{Topic: t}, nil
		}
	}

	client.Close()
	return nil, &ACXTopicValidationError{Message: "Not Found"}
}

// IsAvailable checks for the topic does not exist
func IsAvailable(topic *kafka.TopicSpecification, hosts string) bool {
	client, _ := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": hosts,
	})

	meta, _ := client.GetMetadata(nil, true, 2000)

	for _, t := range meta.Topics {
		if t.Topic == topic.Topic {
			client.Close()
			return false
		}
	}

	client.Close()
	return true
}

func checkAvailability(topics *[]kafka.TopicSpecification, hosts string) *[]ACXCreateTopicError {
	errors := make([]ACXCreateTopicError, 0)

	for _, topic := range *topics {
		if IsAvailable(&topic, hosts) == false {
			errors = append(errors, ACXCreateTopicError{topic, "Unavailable"})
		}
	}

	return &errors
}

func validateTopics(topics *[]kafka.TopicSpecification, hosts string) (bool, []*ACXCreateTopicError) {
	errors := make([]*ACXCreateTopicError, 0)

	for _, topic := range *topics {
		_, error := IsValid(&topic, hosts)

		if error != nil {
			errors = append(errors, &ACXCreateTopicError{topic, "Unavailable"})
		}
	}

	if len(errors) > 0 {
		return false, errors
	}

	return true, nil
}

func parseConfig(config *KafkaConfig) []kafka.TopicSpecification {
	var topics = make([]kafka.TopicSpecification, 0)

	for _, topic := range config.Topics {
		topics = append(topics, kafka.TopicSpecification{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: config.ReplicationFactor,
		})
	}

	return topics
}

func loadConfig(path string) (*KafkaConfig, error) {
	viper.Set("Verbose", true)

	var config KafkaConfig

	viper.SetConfigName("kafka-config.yaml")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		panic("Error in Configuration File")
	}
	viper.Unmarshal(&config)

	return &config, err
}

// CreateTopics create topics based on configuration file
func CreateTopics(config *KafkaConfig) ([]kafka.TopicResult, *[]ACXCreateTopicError) {
	topicSpecs := parseConfig(config)

	errors := checkAvailability(&topicSpecs, strings.Join(config.Hosts, ","))

	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Hosts, ","),
	})

	if err != nil {
		fmt.Printf("Failed Connecting: %v\n", err)
	}

	if len(*errors) > 0 {
		client.Close()
		return nil, errors
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	for _, topicSpec := range topicSpecs {
		fmt.Printf("Creating Topic: %v\n", topicSpec.Topic)
	}

	results, _ := client.CreateTopics(ctx, topicSpecs, kafka.SetAdminOperationTimeout(maxDur))

	client.Close()

	return results, nil
}

// DeleteTopics deletes topics
func DeleteTopics(config *KafkaConfig) ([]kafka.TopicResult, *[]ACXCreateTopicError) {
	topicSpecs := parseConfig(config)

	errors := checkAvailability(&topicSpecs, strings.Join(config.Hosts, ","))

	if len(*errors) < len(topicSpecs) {
		return nil, errors
	}

	client, err := kafka.NewAdminClient(&kafka.ConfigMap{
		"bootstrap.servers": strings.Join(config.Hosts, ","),
	})

	if err != nil {
		fmt.Printf("Failed Connecting: %v\n", err)
	}

	topicNames := make([]string, 0)

	for _, topic := range topicSpecs {
		topicNames = append(topicNames, topic.Topic)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	for _, topicName := range topicNames {
		fmt.Printf("Deleting Topic: %v\n", topicName)
	}

	results, err := client.DeleteTopics(ctx, topicNames, kafka.SetAdminOperationTimeout(maxDur))

	if err != nil {
		fmt.Printf("Failed to delete topics: %v\n", err)

		client.Close()
		return results, &[]ACXCreateTopicError{ACXCreateTopicError{Message: err.Error()}}
	}

	client.Close()
	return results, nil
}
