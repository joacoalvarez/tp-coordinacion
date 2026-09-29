package middleware

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type BaseMiddleware struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	ConsumerTag string
}

func (b *BaseMiddleware) GetConsumerTag(name string) string {
	if b.ConsumerTag != "" {
		return b.ConsumerTag
	}
	return fmt.Sprintf("consumer-%s-%d", name, time.Now().UnixNano())
}

func (b *BaseMiddleware) WrapChannelError(err error) error {
	if err == nil {
		return nil
	}

	if err == amqp.ErrClosed || (b.Channel != nil && b.Channel.IsClosed()) {
		b.ConsumerTag = ""
		return ErrMessageMiddlewareDisconnected
	}

	return ErrMessageMiddlewareMessage
}

func (b *BaseMiddleware) consumeLoop(msgs <-chan amqp.Delivery, callbackFunc func(msg Message, ack func(), nack func())) error {
	for d := range msgs {
		msg := Message{Body: string(d.Body)}
		ack := func() {
			d.Ack(false)
		}
		nack := func() {
			d.Nack(false, true)
		}
		callbackFunc(msg, ack, nack)
	}

	if b.Channel.IsClosed() {
		return ErrMessageMiddlewareDisconnected
	}
	return nil
}

func (b *BaseMiddleware) StopConsuming() error {
	if b.ConsumerTag == "" {
		return nil
	}

	err := b.Channel.Cancel(b.ConsumerTag, false)
	if err != nil {
		return b.WrapChannelError(err)
	}

	b.ConsumerTag = ""
	return nil
}

func (b *BaseMiddleware) Close() error {
	if err := b.Channel.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}

	if err := b.Conn.Close(); err != nil {
		return ErrMessageMiddlewareClose
	}

	return nil
}
