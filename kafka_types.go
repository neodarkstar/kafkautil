package kafkautil

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

// KafkaConfig basic kafka configuration
type KafkaConfig struct {
	Topics            []TopicConfig
	Group             string
	Hosts             []string
	Port              string
	ReplicationFactor int
}

// TopicConfig configuration with acx-plus
type TopicConfig struct {
	Family     string
	Partitions int
	Name       string
}

// ACXCreateTopicResult Results of Create Topic
type ACXCreateTopicResult struct {
	Topic kafka.TopicMetadata
}

// ACXCreateTopicError Create Topic Error
type ACXCreateTopicError struct {
	Topic   kafka.TopicSpecification
	Message string
}

func (e *ACXCreateTopicError) Error() string {
	return "Topic " + e.Topic.Topic + " is " + e.Message
}

// ACXTopicValidationResult contains the result
// of the comparison
type ACXTopicValidationResult struct {
	Topic kafka.TopicMetadata
}

// ACXTopicValidationError records an error when
// comparing the input to the result
type ACXTopicValidationError struct {
	Message string
}

func (e *ACXTopicValidationError) Error() string {
	return e.Message
}
