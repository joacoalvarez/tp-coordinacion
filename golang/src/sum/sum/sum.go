package sum

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"sync"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type SumConfig struct {
	Id                int
	MomHost           string
	MomPort           int
	InputQueue        string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
}

type Sum struct {
	inputQueue      	 middleware.Middleware
	outputExchange  	 middleware.Middleware
	controlProducer 	 middleware.Middleware
	controlConsumer 	 middleware.Middleware
	clientFruitItemMap   map[uint64]map[string]fruititem.FruitItem
	mutex                sync.Mutex
	aggregationKeys      []string
}

func NewSum(config SumConfig) (*Sum, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputExchangeRouteKeys := make([]string, config.AggregationAmount)
	for i := range config.AggregationAmount {
		outputExchangeRouteKeys[i] = fmt.Sprintf("%s_%d", config.AggregationPrefix, i)
	}

	outputExchange, err := middleware.CreateExchangeMiddleware(config.AggregationPrefix, outputExchangeRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	// Internal control Middlewares
	controlProducerRouteKeys := make([]string, 0, config.SumAmount-1)
	for i := range config.SumAmount {
		if i == config.Id {
			continue
		}
		controlProducerRouteKeys = append(controlProducerRouteKeys, fmt.Sprintf("%s_%d", config.SumPrefix, i))
	}
	
	controlProducer, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlProducerRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		return nil, err
	}

	controlConsumerRouteKeys := []string{fmt.Sprintf("%s_%d", config.SumPrefix, config.Id)}
	controlConsumer, err := middleware.CreateExchangeMiddleware(config.SumPrefix, controlConsumerRouteKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		outputExchange.Close()
		controlProducer.Close()
		return nil, err
	}

	return &Sum{
		inputQueue:     inputQueue,
		outputExchange: outputExchange,
		controlProducer: controlProducer,
		controlConsumer: controlConsumer,
		clientFruitItemMap:   map[uint64]map[string]fruititem.FruitItem{},
		aggregationKeys:      outputExchangeRouteKeys,
	}, nil
}

func (sum *Sum) Run() {
	go sum.controlConsumer.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleControlMessage(msg, ack, nack)
	})

	sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
}

func (sum *Sum) handleControlMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing control message", "err", err)
		return
	}

	if isEof {
		slog.Info("Received End Of Records control message")
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
	} else {
		slog.Error("Received unexpected control message")
	}

	slog.Info("Received control message", "clientId", clientId, "fruitRecords", fruitRecords)
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		eofMessage := []fruititem.FruitItem{}
		message, err := inner.SerializeMessage(clientId, eofMessage)
		if err != nil {
			slog.Error("While serializing EOF control message", "err", err)
			return
		}
		if err := sum.controlProducer.Send(*message); err != nil {
			slog.Error("While sending EOF control message", "err", err)
			return
		}
		if err := sum.handleEndOfRecordMessage(clientId); err != nil {
			slog.Error("While handling end of record message", "err", err)
			return
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

func (sum *Sum) handleEndOfRecordMessage(clientId uint64) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	slog.Info("Received End Of Records message")
	for key := range sum.clientFruitItemMap[clientId] {
		fruitRecord := []fruititem.FruitItem{sum.clientFruitItemMap[clientId][key]}
		message, err := inner.SerializeMessage(clientId, fruitRecord)
		if err != nil {
			slog.Debug("While serializing message", "err", err)
			return err
		}
		// Cada fruta va a un único Aggregation, el mismo desde todos los Sum
		if err := sum.outputExchange.SendTo(*message, sum.aggregationKeyFor(key)); err != nil {
			slog.Debug("While sending message", "err", err)
			return err
		}
	}

	// El EOF va a todos los Aggregation
	eofMessage := []fruititem.FruitItem{}
	message, err := inner.SerializeMessage(clientId, eofMessage)
	if err != nil {
		slog.Debug("While serializing EOF message", "err", err)
		return err
	}
	if err := sum.outputExchange.Send(*message); err != nil {
		slog.Debug("While sending EOF message", "err", err)
		return err
	}

	delete(sum.clientFruitItemMap, clientId)
	return nil
}

func (sum *Sum) aggregationKeyFor(fruit string) string {
	hasher := fnv.New32a()
	hasher.Write([]byte(fruit))
	return sum.aggregationKeys[hasher.Sum32()%uint32(len(sum.aggregationKeys))]
}

func (sum *Sum) handleDataMessage(clientId uint64, fruitRecords []fruititem.FruitItem) error {
	sum.mutex.Lock()
	defer sum.mutex.Unlock()

	if _, ok := sum.clientFruitItemMap[clientId]; !ok {
		sum.clientFruitItemMap[clientId] = map[string]fruititem.FruitItem{}
	}
	for _, fruitRecord := range fruitRecords {
		_, ok := sum.clientFruitItemMap[clientId][fruitRecord.Fruit]
		if ok {
			sum.clientFruitItemMap[clientId][fruitRecord.Fruit] = sum.clientFruitItemMap[clientId][fruitRecord.Fruit].Sum(fruitRecord)
		} else {
			sum.clientFruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
	}
	return nil
}
