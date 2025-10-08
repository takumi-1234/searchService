package message

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
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
	consumer          *kafka.Consumer
	producer          *kafka.Producer
	svc               port.SearchService
	logger            *zap.Logger
	topic             string
	deadLetterTopic   string
	maxRetries        int
	initialBackoff    time.Duration
	maxBackoff        time.Duration
	backoffMultiplier float64
}

const (
	defaultMaxRetries        = 3
	defaultInitialBackoff    = 100 * time.Millisecond
	defaultMaxBackoff        = 5 * time.Second
	defaultBackoffMultiplier = 2.0
	producerFlushTimeout     = 5 * time.Second
)

// Option はConsumerのオプションを設定します。
type Option func(*Consumer)

// WithDeadLetterTopic はDLQトピック名を指定します。
func WithDeadLetterTopic(topic string) Option {
	return func(c *Consumer) {
		if topic != "" {
			c.deadLetterTopic = topic
		}
	}
}

// RetryConfig はリトライ動作の設定です。
type RetryConfig struct {
	MaxRetries int
	// InitialBackoff は初回リトライまでの待機時間です。
	InitialBackoff time.Duration
	// MaxBackoff は待機時間の上限です。
	MaxBackoff time.Duration
	// Multiplier は指数バックオフの倍率です。
	Multiplier float64
}

// WithRetryConfig はリトライ設定を適用します。
func WithRetryConfig(cfg RetryConfig) Option {
	return func(c *Consumer) {
		if cfg.MaxRetries >= 0 {
			c.maxRetries = cfg.MaxRetries
		}
		if cfg.InitialBackoff > 0 {
			c.initialBackoff = cfg.InitialBackoff
		}
		if cfg.MaxBackoff > 0 {
			c.maxBackoff = cfg.MaxBackoff
		}
		if cfg.Multiplier > 0 {
			c.backoffMultiplier = cfg.Multiplier
		}
	}
}

// NewConsumer は新しいConsumerインスタンスを生成します。
func NewConsumer(consumer *kafka.Consumer, producer *kafka.Producer, svc port.SearchService, logger *zap.Logger, topic string, opts ...Option) *Consumer {
	c := &Consumer{
		consumer:          consumer,
		producer:          producer,
		svc:               svc,
		logger:            logger,
		topic:             topic,
		deadLetterTopic:   topic + ".dlq",
		maxRetries:        defaultMaxRetries,
		initialBackoff:    defaultInitialBackoff,
		maxBackoff:        defaultMaxBackoff,
		backoffMultiplier: defaultBackoffMultiplier,
	}

	for _, opt := range opts {
		opt(c)
	}

	if c.backoffMultiplier < 1 {
		c.backoffMultiplier = defaultBackoffMultiplier
	}
	if c.initialBackoff <= 0 {
		c.initialBackoff = defaultInitialBackoff
	}
	if c.maxBackoff < c.initialBackoff {
		c.maxBackoff = defaultMaxBackoff
	}

	return c
}

// Run はKafkaコンシューマを開始します。
func (c *Consumer) Run(ctx context.Context) error {
	if err := c.consumer.Subscribe(c.topic, nil); err != nil {
		_ = c.close()
		return err
	}
	c.logger.Info("kafka consumer started", zap.String("topic", c.topic))

	defer func() {
		if err := c.close(); err != nil {
			c.logger.Warn("failed to close kafka consumer cleanly", zap.Error(err))
		}
	}()

	for {
		select {
		case <-ctx.Done():
			c.logger.Info("shutting down kafka consumer")
			return ctx.Err()
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

			if err := c.processMessage(ctx, msg); err != nil {
				c.logger.Error("failed to handle kafka message",
					zap.String("topic", *msg.TopicPartition.Topic),
					zap.ByteString("key", msg.Key),
					zap.Error(err),
				)
			}
		}
	}
}

// processMessage はリトライを伴うメッセージ処理を行います。
func (c *Consumer) processMessage(ctx context.Context, msg *kafka.Message) error {
	var lastErr error
	backoff := c.initialBackoff
	tracer := otel.Tracer("github.com/takumi-1234/searchService/internal/adapter/message")
	topic := ""
	if msg.TopicPartition.Topic != nil {
		topic = *msg.TopicPartition.Topic
	}
	ctx, span := tracer.Start(ctx, "kafka.consumer.process", trace.WithSpanKind(trace.SpanKindConsumer))
	defer span.End()

	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.operation", "process"),
		attribute.String("messaging.destination", topic),
		attribute.String("messaging.destination_kind", "topic"),
		attribute.Int64("messaging.kafka.partition", int64(msg.TopicPartition.Partition)),
		attribute.Int64("messaging.kafka.offset", int64(msg.TopicPartition.Offset)),
	)
	if len(msg.Key) > 0 {
		span.SetAttributes(attribute.String("messaging.kafka.key", string(msg.Key)))
	}

	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if ctx.Err() != nil {
			span.SetStatus(codes.Error, "context cancelled")
			return ctx.Err()
		}

		lastErr = c.handleMessage(ctx, msg)
		if lastErr == nil {
			if attempt > 0 {
				c.logger.Info("message processed after retry",
					zap.String("topic", *msg.TopicPartition.Topic),
					zap.Int("attempts", attempt+1),
				)
				span.AddEvent("message processed after retry",
					trace.WithAttributes(attribute.Int("messaging.kafka.retry_count", attempt)),
				)
			}
			span.SetAttributes(attribute.Int("messaging.kafka.retry_count", attempt))
			span.SetStatus(codes.Ok, "processed")
			return nil
		}

		span.RecordError(lastErr, trace.WithAttributes(
			attribute.Int("messaging.kafka.retry_attempt", attempt),
		))

		if attempt == c.maxRetries {
			c.logger.Error("exhausted message retries, sending to DLQ",
				zap.String("topic", *msg.TopicPartition.Topic),
				zap.Int("attempts", attempt+1),
				zap.Error(lastErr),
			)

			if err := c.sendToDeadLetter(ctx, msg, lastErr); err != nil {
				c.logger.Error("failed to send message to DLQ", zap.Error(err))
				span.RecordError(err, trace.WithAttributes(attribute.String("phase", "dlq")))
				span.SetStatus(codes.Error, err.Error())
				return errors.Join(lastErr, err)
			}
			span.AddEvent("message sent to DLQ")
			span.SetStatus(codes.Error, lastErr.Error())
			return lastErr
		}

		c.logger.Warn("message processing failed, will retry",
			zap.String("topic", *msg.TopicPartition.Topic),
			zap.Int("attempt", attempt+1),
			zap.Duration("backoff", backoff),
			zap.Error(lastErr),
		)
		span.AddEvent("message processing failed",
			trace.WithAttributes(
				attribute.Int("messaging.kafka.retry_attempt", attempt),
				attribute.String("error", lastErr.Error()),
			),
		)

		select {
		case <-ctx.Done():
			span.SetStatus(codes.Error, "context cancelled while waiting for retry")
			return ctx.Err()
		case <-time.After(backoff):
		}

		nextBackoff := time.Duration(float64(backoff) * c.backoffMultiplier)
		if nextBackoff > c.maxBackoff {
			nextBackoff = c.maxBackoff
		}
		backoff = nextBackoff
	}

	return lastErr
}

func (c *Consumer) sendToDeadLetter(ctx context.Context, msg *kafka.Message, procErr error) error {
	if c.producer == nil {
		return errors.New("dlq producer not configured")
	}

	deliveryChan := make(chan kafka.Event, 1)
	headers := append([]kafka.Header{}, msg.Headers...)
	if msg.TopicPartition.Topic != nil {
		headers = append(headers, kafka.Header{
			Key:   "x-original-topic",
			Value: []byte(*msg.TopicPartition.Topic),
		})
	}
	headers = append(headers,
		kafka.Header{Key: "x-original-partition", Value: []byte(strconv.Itoa(int(msg.TopicPartition.Partition)))},
		kafka.Header{Key: "x-original-offset", Value: []byte(strconv.FormatInt(int64(msg.TopicPartition.Offset), 10))},
		kafka.Header{Key: "x-error", Value: []byte(procErr.Error())},
	)

	dlqTopic := c.deadLetterTopic
	if err := c.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &dlqTopic, Partition: kafka.PartitionAny},
		Key:            msg.Key,
		Value:          msg.Value,
		Headers:        headers,
	}, deliveryChan); err != nil {
		close(deliveryChan)
		return err
	}

	select {
	case event := <-deliveryChan:
		if m, ok := event.(*kafka.Message); ok {
			return m.TopicPartition.Error
		}
	case <-ctx.Done():
		// ensure delivery channel is drained to avoid blocking the producer goroutine
		select {
		case event := <-deliveryChan:
			if m, ok := event.(*kafka.Message); ok {
				if tpErr := m.TopicPartition.Error; tpErr != nil {
					return errors.Join(ctx.Err(), tpErr)
				}
			}
		case <-time.After(time.Second):
		}
		return ctx.Err()
	}

	return nil
}

func (c *Consumer) close() error {
	var closeErr error
	if c.consumer != nil {
		if err := c.consumer.Close(); err != nil {
			closeErr = err
		}
	}

	if c.producer != nil {
		flushTimeout := int(producerFlushTimeout / time.Millisecond)
		if flushTimeout < 0 {
			flushTimeout = 0
		}
		_ = c.producer.Flush(flushTimeout)
		c.producer.Close()
	}

	return closeErr
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
