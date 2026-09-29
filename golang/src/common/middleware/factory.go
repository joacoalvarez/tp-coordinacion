package middleware

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func commonMiddleware(connectionSettings ConnSettings) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(
		fmt.Sprintf("amqp://guest:guest@%s:%d/",
			connectionSettings.Hostname,
			connectionSettings.Port,
		))
	if err != nil {
		return nil, nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, nil, err
	}

	return conn, ch, nil
}

func CreateQueueMiddleware(queueName string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := commonMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	qMiddleware := QueueMiddleware{
		BaseMiddleware: BaseMiddleware{
			Conn:    conn,
			Channel: ch,
		},
		Queue: q,
	}

	return &qMiddleware, nil
}

func CreateExchangeMiddleware(exchange string, keys []string, connectionSettings ConnSettings) (Middleware, error) {
	conn, ch, err := commonMiddleware(connectionSettings)
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"direct",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	eMiddleware := ExchangeMiddleware{
		BaseMiddleware: BaseMiddleware{
			Conn:    conn,
			Channel: ch,
		},
		ExchangeName: exchange,
		RouteKeys:    keys,
	}

	return &eMiddleware, nil
}
