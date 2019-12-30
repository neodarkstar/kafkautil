package kafkautil

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

// ACXKafkaClient Wrapper on Kafka Client with ACX Specific functions
type ACXKafkaClient struct {
	AdminClient *kafka.AdminClient
	topicSpecs  []kafka.TopicSpecification
}

// NewACXKafkaClient Creates a new KafkaClient
func (a *ACXKafkaClient) NewACXKafkaClient(client *kafka.AdminClient, config *KafkaConfig) *ACXKafkaClient {
	a.topicSpecs = make([]kafka.TopicSpecification, 0)

	for _, topic := range config.Topics {
		a.topicSpecs = append(a.topicSpecs, kafka.TopicSpecification{
			Topic:             topic.Name,
			NumPartitions:     topic.Partitions,
			ReplicationFactor: config.ReplicationFactor,
		})
	}

	return &ACXKafkaClient{
		AdminClient: client,
	}
}

// UnmarshalConfig takes in a path and loads the KafkaConfig
func UnmarshalConfig(path string, fileName string) *KafkaConfig {
	viper.Set("Verbose", true)

	var config KafkaConfig

	viper.SetConfigName(fileName)
	viper.AddConfigPath(path)

	err := viper.ReadInConfig()
	if err != nil {
		panic("Error in Configuration File")
	}

	viper.Unmarshal(&config)

	return &config
}

func (a *ACXKafkaClient) verifyTopic(topic *kafka.TopicSpecification) (*ACXTopicValidationResult, *ACXTopicValidationError) {
	meta, _ := a.AdminClient.GetMetadata(nil, true, 2000)

	for _, t := range meta.Topics {
		if t.Topic == topic.Topic && len(t.Partitions) == topic.NumPartitions {
			return &ACXTopicValidationResult{Topic: t}, nil
		}
	}

	return nil, &ACXTopicValidationError{Message: "Not Found"}
}

func (a *ACXKafkaClient) topicExists(topic *kafka.TopicSpecification) bool {
	meta, _ := a.AdminClient.GetMetadata(nil, true, 2000)

	for _, t := range meta.Topics {
		if t.Topic == topic.Topic {
			return false
		}
	}

	return true
}

func (a *ACXKafkaClient) verifyTopics() *[]ACXCreateTopicError {
	errors := make([]ACXCreateTopicError, 0)

	for _, topicSpec := range a.topicSpecs {
		if a.topicExists(&topicSpec) == false {
			errors = append(errors, ACXCreateTopicError{topicSpec, "Unavailable"})
		}
	}

	return &errors
}

func (a *ACXKafkaClient) checkAvailability() *[]ACXCreateTopicError {
	errors := make([]ACXCreateTopicError, 0)

	for _, topicSpec := range a.topicSpecs {
		_, error := a.verifyTopic(&topicSpec)

		if error != nil {
			errors = append(errors, ACXCreateTopicError{topicSpec, "Unavailable"})
		}
	}

	if len(errors) > 0 {
		return &errors
	}

	return nil
}

// CreateTopics create topics based on configuration file
func (a *ACXKafkaClient) CreateTopics() []kafka.TopicResult {
	err := a.checkAvailability()

	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	for _, topicSpec := range a.topicSpecs {
		fmt.Printf("Creating Topic: %v\n", topicSpec.Topic)
	}

	results, error := a.AdminClient.CreateTopics(ctx, a.topicSpecs, kafka.SetAdminOperationTimeout(maxDur))

	if error != nil {
		panic(err)
	}

	return results
}

// DeleteTopics deletes topics
func (a *ACXKafkaClient) DeleteTopics(config *KafkaConfig) []kafka.TopicResult {
	errors := a.checkAvailability()

	if len(*errors) < len(a.topicSpecs) {
		panic(errors)
	}

	topicNames := make([]string, 0)

	for _, topic := range a.topicSpecs {
		topicNames = append(topicNames, topic.Topic)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	for _, topicName := range topicNames {
		fmt.Printf("Deleting Topic: %v\n", topicName)
	}

	results, err := a.AdminClient.DeleteTopics(ctx, topicNames, kafka.SetAdminOperationTimeout(maxDur))

	if err != nil {
		panic(err)
	}

	return results
}
