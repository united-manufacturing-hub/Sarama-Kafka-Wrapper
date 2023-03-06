# Sarama Kafka Wrapper

An opinionated wrapper around the Sarama Kafka client.

## Usage

Use `NewKafkaClient` to create a new Kafka client.

### Sending Messages
Use `client.EnqueueMessage` to enqueue a message.

### Receiving Messages
Use `client.GetMessages` to get a channel of messages.

### Closing
Use `client.Close` to cleanly close the client.

