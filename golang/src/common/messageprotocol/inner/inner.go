package inner

import (
	"encoding/json"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type message struct {
	ClientId  uint64
	Data      []fruititem.FruitItem
	EofSeenBy []int
}

func serializeJson(msg message) ([]byte, error) {
	return json.Marshal(msg)
}

func deserializeJson(body []byte) (message, error) {
	var msg message
	if err := json.Unmarshal(body, &msg); err != nil {
		return message{}, err
	}
	return msg, nil
}

func SerializeMessage(id uint64, fruitRecords []fruititem.FruitItem) (*middleware.Message, error) {
	body, err := serializeJson(message{ClientId: id, Data: fruitRecords})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeMessage(msg *middleware.Message) (uint64, []fruititem.FruitItem, bool, error) {
	decoded, err := deserializeJson([]byte(msg.Body))
	if err != nil {
		return 0, nil, false, err
	}
	return decoded.ClientId, decoded.Data, len(decoded.Data) == 0, nil
}

// EOF used by sum to check if all have finished
func SerializeEOFMessage(id uint64, eofSeenBy []int) (*middleware.Message, error) {
	body, err := serializeJson(message{ClientId: id, Data: []fruititem.FruitItem{}, EofSeenBy: eofSeenBy})
	if err != nil {
		return nil, err
	}
	return &middleware.Message{Body: string(body)}, nil
}

func DeserializeEOFMessage(msg *middleware.Message) (uint64, []int, error) {
	decoded, err := deserializeJson([]byte(msg.Body))
	if err != nil {
		return 0, nil, err
	}
	return decoded.ClientId, decoded.EofSeenBy, nil
}
