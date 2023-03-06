package kafka

import jsoniter "github.com/json-iterator/go"

var json = jsoniter.ConfigCompatibleWithStandardLibrary

type KafkaMessage struct {
	Topic string
	Value []byte
}

func ToKafkaMessage(topic string, message interface{}) (KafkaMessage, error) {
	bytes, err := json.Marshal(message)
	if err != nil {
		return KafkaMessage{}, err
	}

	return KafkaMessage{
		Topic: topic,
		Value: bytes,
	}, nil
}
