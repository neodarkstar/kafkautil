package kafkautil

import (
	"context"
	"fmt"
	"time"

	"github.com/spf13/viper"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

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

// ACXKafkaClient Wrapper on Kafka Client with ACX Specific functions
type ACXKafkaClient struct {
	AdminClient *kafka.AdminClient
}

// ValidateTopic Checks if the topic exists and the partition count is correct
func (a *ACXKafkaClient) validateTopic(topic *kafka.TopicSpecification, meta *kafka.Metadata) *ACXTopicValidationResult {
	topicMeta := meta.Topics[topic.Topic]

	if topicMeta.Topic == "" {
		return &ACXTopicValidationResult{&topicMeta, &ACXTopicValidationError{Message: "Topic Not Found", Topic: topic}}
	}

	if len(topicMeta.Partitions) != topic.NumPartitions {
		return &ACXTopicValidationResult{&topicMeta, &ACXTopicValidationError{Message: "Incorrect Partion Count", Topic: topic}}
	}

	return &ACXTopicValidationResult{&topicMeta, &ACXTopicValidationError{}}
}

// ValidateTopic Checks if the topic exists and the partition count is correct
func (a *ACXKafkaClient) exists(topicSpecs []kafka.TopicSpecification) (bool, *kafka.TopicMetadata) {
	const allTopics = true

	meta, error := a.AdminClient.GetMetadata(nil, allTopics, 2000)
	if error != nil {
		panic(error)
	}

	for _, topicSpec := range topicSpecs {
		topicMeta := meta.Topics[topicSpec.Topic]

		if topicMeta.Topic != "" {
			return true, &topicMeta
		}
	}

	return false, nil
}

// ValidateTopics analyzes created topics to match the specs passed in
func (a *ACXKafkaClient) ValidateTopics(topicSpecs []kafka.TopicSpecification) (*[]ACXTopicValidationResult, bool) {
	const allTopics = true
	results := make([]ACXTopicValidationResult, 0)
	var success = true

	meta, error := a.AdminClient.GetMetadata(nil, allTopics, 2000)
	if error != nil {
		panic(error)
	}

	for _, topicSpec := range topicSpecs {
		result := a.validateTopic(&topicSpec, meta)

		results = append(results, *result)

		if result.Error.Error() != "Success" {
			success = false
		}
	}

	return &results, success
}

// CreateTopics create topics based on configuration file
func (a *ACXKafkaClient) CreateTopics(topicSpecs []kafka.TopicSpecification) []kafka.TopicResult {
	exists, topicMeta := a.exists(topicSpecs)

	if exists == true {
		fmt.Println("Topic " + topicMeta.Topic + " Exists")
		return make([]kafka.TopicResult, 0)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	results, error := a.AdminClient.CreateTopics(ctx, topicSpecs, kafka.SetAdminOperationTimeout(maxDur))

	if error != nil {
		panic(error)
	}

	return results
}

// DeleteTopics deletes topics
func (a *ACXKafkaClient) DeleteTopics(topicSpecs []kafka.TopicSpecification) []kafka.TopicResult {
	topicNames := make([]string, 0)

	for _, topic := range topicSpecs {
		topicNames = append(topicNames, topic.Topic)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maxDur, _ := time.ParseDuration("60s")

	results, err := a.AdminClient.DeleteTopics(ctx, topicNames, kafka.SetAdminOperationTimeout(maxDur))

	if err != nil {
		panic(err)
	}

	return results
}
