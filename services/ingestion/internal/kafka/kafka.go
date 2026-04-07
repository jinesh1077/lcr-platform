package kafka

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/IBM/sarama"
	"github.com/carrier-opt/ingestion/internal/models"
)

const TopicRatesActivated = "rates.activated"

type Producer struct {
	producer sarama.SyncProducer
}

func NewProducer(brokers []string) (*Producer, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	p, err := sarama.NewSyncProducer(brokers, cfg)
	if err != nil {
		return nil, err
	}
	return &Producer{producer: p}, nil
}

func (p *Producer) Close() error {
	return p.producer.Close()
}

func (p *Producer) PublishRatesActivated(ctx context.Context, event models.RatesActivatedEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: TopicRatesActivated,
		Key:   sarama.StringEncoder(event.RateSheetID.String()),
		Value: sarama.ByteEncoder(body),
	}
	_, _, err = p.producer.SendMessage(msg)
	return err
}

func NewConsumer(brokers []string, group string) (sarama.ConsumerGroup, error) {
	cfg := sarama.NewConfig()
	cfg.Consumer.Group.Rebalance.GroupStrategies = []sarama.BalanceStrategy{sarama.NewBalanceStrategyRoundRobin()}
	cfg.Consumer.Offsets.Initial = sarama.OffsetOldest
	return sarama.NewConsumerGroup(brokers, group, cfg)
}

func EnsureTopics(brokers []string) error {
	cfg := sarama.NewConfig()
	admin, err := sarama.NewClusterAdmin(brokers, cfg)
	if err != nil {
		return err
	}
	defer admin.Close()

	topics := []string{"cdr.events", "cdr.events.dlq", TopicRatesActivated}
	for _, t := range topics {
		err := admin.CreateTopic(t, &sarama.TopicDetail{
			NumPartitions:     3,
			ReplicationFactor: 1,
		}, false)
		if err != nil && !topicExists(err) {
			return err
		}
	}
	return nil
}

func topicExists(err error) bool {
	return err != nil && (err.Error() == "Topic already exists" ||
		fmt.Sprintf("%v", err) == "Topic already exists")
}
