package kafka

import (
	"context"
	"errors"
	"github.com/OneOfOne/xxhash"
	"github.com/Shopify/sarama"
	"go.uber.org/zap"
	"regexp"
	"sort"
	"sync"
	"time"
)

type Client struct {
	consumer             sarama.ConsumerGroup
	producer             sarama.AsyncProducer
	admin                sarama.ClusterAdmin
	open                 bool
	messageQueue         chan KafkaMessage
	topics               []string
	topicsRWMutex        *sync.RWMutex
	topicCheckSum        uint64
	topicRegex           *regexp.Regexp
	lastCheck            int64
	incomingMessageQueue chan KafkaMessage
	ready                bool
	closing              bool
}

type NewClientOptions struct {
	Brokers          []string
	ConsumerName     string
	ListenTopicRegex *regexp.Regexp
}

var topicCache = make(map[string]bool)
var topicCacheMutex = new(sync.RWMutex)
var lastTopicChange = time.Now().UnixNano()

func NewCryptKafkaClient(opts NewClientOptions) (client *Client, err error) {
	config := sarama.NewConfig()
	config.ClientID = opts.ConsumerName
	config.Producer.Return.Errors = true
	config.Consumer.Offsets.Initial = sarama.OffsetNewest

	client = &Client{}

	client.consumer, err = sarama.NewConsumerGroup(opts.Brokers, opts.ConsumerName, config)
	if err != nil {
		return nil, err
	}

	client.producer, err = sarama.NewAsyncProducer(opts.Brokers, config)

	if err != nil {
		return nil, err
	}

	client.admin, err = sarama.NewClusterAdmin(opts.Brokers, config)

	if err != nil {
		return nil, err
	}

	client.topicRegex = opts.ListenTopicRegex
	client.open = true
	client.messageQueue = make(chan KafkaMessage, 1000)

	client.incomingMessageQueue = make(chan KafkaMessage)

	client.topicsRWMutex = &sync.RWMutex{}
	client.ready = false

	client.populateTopics()

	go client.errorReader()
	go client.messageSender()
	go client.refreshTopics()
	go client.subscriber()
	return client, nil
}

func (c *Client) Close() error {
	if !c.open || c.closing {
		return nil
	}
	c.closing = true

	err := c.consumer.Close()
	if err != nil {
		return err
	}

	ctx10Sec, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for len(c.messageQueue) > 0 {
		zap.S().Debugf("Awaiting send queue to be empty. %d messages left", len(c.messageQueue))
		select {
		case <-ctx10Sec.Done():
			zap.S().Errorf("Failed to empty send queue in time. %d messages left", len(c.messageQueue))
		default:
			time.Sleep(100 * time.Millisecond)
		}
	}

	err = c.producer.Close()
	if err != nil {
		return err
	}
	err = c.admin.Close()
	if err != nil {
		return err
	}
	c.open = false

	return nil
}

func (c *Client) topicCreator(topic string) (err error) {
	var exists bool
	exists, err = c.checkIfTopicExists(topic)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	topicCacheMutex.Lock()
	topicCache[topic] = false
	topicCacheMutex.Unlock()
	err = c.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     1,
		ReplicationFactor: 1,
	}, false)
	return err
}

func (c *Client) checkIfTopicExists(topic string) (exists bool, err error) {
	topicCacheMutex.RLock()
	initialized, ok := topicCache[topic]
	topicCacheMutex.RUnlock()
	if !ok || !initialized {
		var topics map[string]sarama.TopicDetail
		topics, err = c.admin.ListTopics()
		if err != nil {
			return false, err
		}
		_, exists = topics[topic]
		topicCacheMutex.Lock()
		topicCache[topic] = exists
		lastTopicChange = time.Now().UnixNano()
		zap.S().Infof("Topic cache updated: %s - %v", topic, exists)
		topicCacheMutex.Unlock()
		return exists, nil
	}

	return initialized, nil
}

func (c *Client) messageSender() {
	for c.open {
		select {
		case msg := <-c.messageQueue:
			// Check if topic exists
			exists, err := c.checkIfTopicExists(msg.Topic)
			if err != nil {
				zap.S().Errorf("Error checking if topic exists: %v", err)
				// re-queue message
				c.messageQueue <- msg
				time.Sleep(5 * time.Second)
				continue
			}
			if !exists {
				zap.S().Errorf("Topic %s does not exist yet", msg.Topic)
				// re-queue message
				c.messageQueue <- msg
				time.Sleep(1 * time.Second)
				continue
			}

			c.producer.Input() <- &sarama.ProducerMessage{
				Topic:    msg.Topic,
				Value:    sarama.ByteEncoder(msg.Value),
				Metadata: sarama.MetadataRequest{AllowAutoTopicCreation: true},
			}
		case <-time.After(5 * time.Second):
			continue
		}
	}
}

func (c *Client) EnqueueMessage(msg KafkaMessage) (err error) {
	if !c.open {
		return errors.New("client is closed")
	}

	err = c.topicCreator(msg.Topic)
	if err != nil {
		return err
	}

	// try insert to messageQueue, if not full, insert, else return error
	select {
	case c.messageQueue <- msg:
		return nil
	default:
		return errors.New("message queue is full")
	}
}

func (c *Client) errorReader() {
	for c.open {
		select {
		case err := <-c.producer.Errors():
			zap.S().Errorf("Error writing message: %v", err)
		// Recheck c.open every 5 seconds
		case <-time.After(5 * time.Second):
			continue
		}
	}
}

func (c *Client) getTopicsForRegex() []string {
	if !c.open {
		return nil
	}

	var subTopics []string
	topicCacheMutex.RLock()
	for topic, initialized := range topicCache {
		if initialized && c.topicRegex.MatchString(topic) {
			subTopics = append(subTopics, topic)
		}
	}
	topicCacheMutex.RUnlock()

	// sort topics
	sort.Strings(subTopics)

	return subTopics
}

func (c *Client) refreshTopics() {
	c.lastCheck = int64(0)
	for c.open {
		if lastTopicChange > c.lastCheck {
			c.lastCheck = time.Now().UnixNano()

			zap.S().Infof("Refreshing topics")

			subTopics := c.getTopicsForRegex()
			// Generate hash by iterating over topics and appending using xxhash64
			hasher := xxhash.New64()
			for _, topic := range subTopics {
				_, err := hasher.WriteString(topic)
				if err != nil {
					return
				}
			}
			checkSum := hasher.Sum64()

			if checkSum != c.topicCheckSum {
				c.topicsRWMutex.Lock()
				c.topicCheckSum = checkSum
				c.topics = subTopics
				c.topicsRWMutex.Unlock()
				zap.S().Infof("Subscribable-topics updated: %v", subTopics)
			}
		} else {
			time.Sleep(5 * time.Second)
		}
	}
}

func (c *Client) ChangeSubscribedTopics(newTopicRegex *regexp.Regexp) {
	c.topicRegex = newTopicRegex
	c.lastCheck = 0
}

func (c *Client) subscriber() {
	lastHash := uint64(0)
	var cancel context.CancelFunc
	var ctx context.Context
	for c.open {
		if lastHash != c.topicCheckSum {
			c.ready = false
			if cancel != nil {
				cancel()
			}
			if len(c.topics) == 0 {
				zap.S().Infof("No topics to subscribe to")
				time.Sleep(5 * time.Second)
				continue
			}
			zap.S().Infof("Subscribing to topics: %v", c.topics)
			ctx, cancel = context.WithCancel(context.Background())
			go c.GSub(ctx, cancel)
			c.ready = true
		}
		time.Sleep(5 * time.Second)
	}
	cancel()
}

func (c *Client) GSub(ctx context.Context, cncl context.CancelFunc) {
	zap.S().Infof("Starting consumer group")

	consumerGroup := NewConsumerGroupHandler(&c.incomingMessageQueue)
	err := c.consumer.Consume(ctx, c.topics, consumerGroup)
	if err != nil {
		zap.S().Errorf("Error consuming topics: %v", err)
		cncl()
	}
}

func (c *Client) GetMessages() <-chan KafkaMessage {
	return c.incomingMessageQueue
}

func (c *Client) Ready() bool {
	return c.ready
}

func (c *Client) populateTopics() {
	if !c.open {
		return
	}
	topicCacheMutex.Lock()
	defer topicCacheMutex.Unlock()
	if len(topicCache) > 0 {
		return
	}
	topics, err := c.admin.ListTopics()
	if err != nil {
		zap.S().Errorf("Error listing topics: %v", err)
		return
	}
	for topic := range topics {
		topicCache[topic] = true
	}
}

type ConsumerGroupHandler struct {
	queue *chan KafkaMessage
}

func (c ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	zap.S().Infof("Setup consumer group session: %v", session)
	return nil
}

func (c ConsumerGroupHandler) Cleanup(session sarama.ConsumerGroupSession) error {
	zap.S().Infof("Cleanup consumer group session: %v", session)
	session.Commit()
	return nil
}

func (c ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	zap.S().Infof("Consuming claim: %s - %d", claim.Topic(), claim.InitialOffset())
	messages := 0
	for message := range claim.Messages() {
		zap.S().Debugf("Message claimed: %v", message)
		*c.queue <- KafkaMessage{
			Topic: message.Topic,
			Value: message.Value,
		}
		session.MarkMessage(message, "")
		messages++
	}
	zap.S().Infof("Done consuming claim: %v - %d messages", claim, messages)
	return nil
}

func NewConsumerGroupHandler(queue *chan KafkaMessage) sarama.ConsumerGroupHandler {
	return &ConsumerGroupHandler{
		queue: queue,
	}
}
