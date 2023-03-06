package kafka

import (
	"go.elastic.co/ecszap"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"regexp"
	"testing"
	"time"
)

func TestAAAAInitZap(t *testing.T) {
	initZAP()
}

func TestSendMessage(t *testing.T) {

	opts := NewClientOptions{
		Brokers:          []string{"localhost:9093"},
		ConsumerName:     "test",
		ListenTopicRegex: regexp.MustCompile("test"),
	}
	c, err := NewKafkaClient(opts)
	if err != nil {
		t.Fatalf("Error creating client: %v", err)
	}

	// Wait for client to be ready
	for !c.Ready() {
		time.Sleep(100 * time.Millisecond)
	}

	for i := 0; i < 100; i++ {
		err = c.EnqueueMessage(KafkaMessage{
			Topic: "test",
			Value: []byte("testValue"),
		})
		if err != nil {
			t.Errorf("Error writing message: %v", err)
		}
	}

	msg := c.GetMessages()

	// Consumer should have received the message

	var received uint64
out:
	for {
		select {
		case m := <-msg:
			t.Logf("Received message: %v", m)
			received++
		case <-time.After(5 * time.Second):
			t.Logf("No more messages received")
			break out
		}
	}

	t.Logf("Received %d messages", received)

	time.Sleep(5 * time.Second)

	defer c.Close()
}

func TestZZZZZZShutdownZap(t *testing.T) {
	if err := logger.Sync(); err != nil {
		panic(err)
	}
}

var logger *zap.Logger

// Init initializes the logger
func initZAP() {
	encoderConfig := ecszap.EncoderConfig{
		EnableStackTrace: true,
		EnableCaller:     true,
		EncodeLevel:      zapcore.CapitalColorLevelEncoder,
		EncodeDuration:   zapcore.NanosDurationEncoder,
		EncodeCaller:     ecszap.FullCallerEncoder,
	}
	logger = zap.New(
		ecszap.NewCore(encoderConfig, os.Stdout, zap.DebugLevel),
		zap.AddCaller(),
		zap.AddStacktrace(zapcore.ErrorLevel))
	zap.ReplaceGlobals(logger)
}
