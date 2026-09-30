package middleware

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	BaseMiddleware
	Queue amqp.Queue
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if q.ConsumerTag != "" {
		return nil
	}

	// unique tag for each consumer
	tag := q.GetConsumerTag(q.Queue.Name)

	msgs, err := q.Channel.Consume(
		q.Queue.Name,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return q.WrapChannelError(err)
	}

	q.ConsumerTag = tag
	return q.consumeLoop(msgs, callbackFunc)
}

func (q *QueueMiddleware) Send(msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := msg.Body
	err := q.Channel.PublishWithContext(
		ctx,
		"",
		q.Queue.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		return q.WrapChannelError(err)
	}

	return nil
}

// Una cola no tiene routing keys: SendTo no está soportado.
func (q *QueueMiddleware) SendTo(msg Message, key string) error {
	return ErrMessageMiddlewareMessage
}
