package message

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"

	"github.com/takumi-1234/searchService/internal/port"
)

// DocumentIndexRequest はKafkaメッセージのペイロードスキーマです。
type DocumentIndexRequest struct {
	Payload struct {
		IndexName  string                 `json:"index_name"`
		DocumentID string                 `json:"document_id"`
		Action     string                 `json:"action"`
		Fields     map[string]interface{} `json:"fields"`
		Vector     []float32              `json:"vector"`
	} `json:"payload"`
}

// Consumer はKafkaメッセージを消費し、ビジネスロジックを呼び出します。
type Consumer struct {
	consumer *kafka.Consumer
	svc      port.SearchService
	logger   *zap.Logger
	topic    string
}

// NewConsumer は新しいConsumerインスタンスを生成します。
func NewConsumer(consumer *kafka.Consumer, svc port.SearchService, logger *zap.Logger, topic string) *Consumer {
	return &Consumer{
		consumer: consumer,
		svc:      svc,
		logger:   logger,
		topic:    topic,
	}
}

// Run はKafkaコンシューマを開始します。
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		return err
	}
	c.logger.Info("kafka consumer started", zap.String("topic", c.topic))

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("shutting down kafka consumer")
			return c.consumer.Close()
		default:
			msg, err := c.consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				var kafkaErr kafka.Error
				if errors.As(err, &kafkaErr) && kafkaErr.Code() == kafka.ErrTimedOut {
					// タイムアウトエラーは無視
					continue
				}
				c.logger.Error("kafka read error", zap.Error(err))
				continue
			}

			if err := c.handleMessage(ctx, msg); err != nil {
				c.logger.Error("failed to handle kafka message",
					zap.String("topic", *msg.TopicPartition.Topic),
					zap.ByteString("key", msg.Key),
					zap.Error(err),
				)
			}
		}
	}
}

// handleMessage は単一のKafkaメッセージを処理します。
func (c *Consumer) handleMessage(ctx context.Context, msg *kafka.Message) error {
	var req DocumentIndexRequest
	if err := json.Unmarshal(msg.Value, &req); err != nil {
		return err
	}

	payload := req.Payload
	switch payload.Action {
	case "UPSERT":
		params := port.IndexDocumentParams{
			IndexName:  payload.IndexName,
			DocumentID: payload.DocumentID,
			Fields:     payload.Fields,
			Vector:     payload.Vector,
		}
		return c.svc.IndexDocument(ctx, params)
	case "DELETE":
		return c.svc.DeleteDocument(ctx, payload.IndexName, payload.DocumentID)
	default:
		c.logger.Warn("unknown action in kafka message", zap.String("action", payload.Action))
	}

	return nil
}
