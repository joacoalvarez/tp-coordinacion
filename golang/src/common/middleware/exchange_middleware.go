package middleware

import (
	"context"
	"slices"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ExchangeMiddleware struct {
	BaseMiddleware
	ExchangeName string
	RouteKeys    []string
}

func (e *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if e.ConsumerTag != "" {
		return nil
	}

	// unique tag for each consumer
	tag := e.GetConsumerTag(e.ExchangeName)

	q, err := e.Channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return e.WrapChannelError(err)
	}

	for _, key := range e.RouteKeys {
		err = e.Channel.QueueBind(
			q.Name,
			key,
			e.ExchangeName,
			false,
			nil)
		if err != nil {
			return e.WrapChannelError(err)
		}
	}

	msgs, err := e.Channel.Consume(
		q.Name,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return e.WrapChannelError(err)
	}

	e.ConsumerTag = tag
	return e.consumeLoop(msgs, callbackFunc)
}

func (e *ExchangeMiddleware) Send(msg Message) error {
	for _, key := range e.RouteKeys {
		if err := e.publish(msg, key); err != nil {
			return err
		}
	}
	return nil
}

func (e *ExchangeMiddleware) SendTo(msg Message, key string) error {
	if !slices.Contains(e.RouteKeys, key) {
		return ErrMessageMiddlewareMessage
	}
	return e.publish(msg, key)
}

func (e *ExchangeMiddleware) publish(msg Message, key string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := e.Channel.PublishWithContext(
		ctx,
		e.ExchangeName,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(msg.Body),
		})
	if err != nil {
		return e.WrapChannelError(err)
	}
	return nil
}
