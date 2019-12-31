package kafkautil

import "gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"

// KafkaConfig basic kafka configuration
type KafkaConfig struct {
	Topics []kafka.TopicSpecification
	Group  string
	Hosts  []string
	Port   string
}

// ACXCreateTopicResult Results of Create Topic
type ACXCreateTopicResult struct {
	Topic kafka.TopicMetadata
	Error *ACXCreateTopicError
}

// ACXCreateTopicError Create Topic Error
type ACXCreateTopicError struct {
	Topic   kafka.TopicSpecification
	Message string
}

func (e *ACXCreateTopicError) Error() string {
	if e.Message == "" {
		return "Success"
	}
	return "Topic " + e.Topic.Topic + " is " + e.Message
}

// ACXTopicValidationResult contains the result
// of the comparison
type ACXTopicValidationResult struct {
	Topic *kafka.TopicMetadata
	Error *ACXTopicValidationError
}

// ACXTopicValidationError records an error when
// comparing the input to the result
type ACXTopicValidationError struct {
	Message string
	Topic   *kafka.TopicSpecification
}

func (e *ACXTopicValidationError) Error() string {
	if e.Message == "" {
		return "Success"
	}

	return e.Message
}
