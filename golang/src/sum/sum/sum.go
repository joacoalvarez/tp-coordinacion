package sum

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"hash/fnv"
	"log/slog"
	"slices"

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
	id                 int
	sumAmount          int
	inputQueue         middleware.Middleware
	outputExchange     middleware.Middleware
	clientFruitItemMap map[uint64]map[string]fruititem.FruitItem
	aggregationKeys    []string
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

	return &Sum{
		id:                 config.Id,
		sumAmount:          config.SumAmount,
		inputQueue:         inputQueue,
		outputExchange:     outputExchange,
		clientFruitItemMap: map[uint64]map[string]fruititem.FruitItem{},
		aggregationKeys:    outputExchangeRouteKeys,
	}, nil
}

func (sum *Sum) Run() {
	go sum.handleSignals()

	err := sum.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sum.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("While consuming messages", "err", err)
	}

	sum.inputQueue.Close()
	sum.outputExchange.Close()
}

func (sum *Sum) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	sum.inputQueue.StopConsuming()
}

func (sum *Sum) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}

	if isEof {
		if err := sum.handleEndOfRecordMessage(&msg); err != nil {
			slog.Error("While handling end of record message", "err", err)
		}
		return
	}

	if err := sum.handleDataMessage(clientId, fruitRecords); err != nil {
		slog.Error("While handling data message", "err", err)
	}
}

// EOF resent via input_queue until all Sums have proccessed it
func (sum *Sum) handleEndOfRecordMessage(msg *middleware.Message) error {
	clientId, eofSeenBy, err := inner.DeserializeEOFMessage(msg)
	if err != nil {
		return err
	}

	if !slices.Contains(eofSeenBy, sum.id) {
		if err := sum.flushClient(clientId); err != nil {
			return err
		}
		eofSeenBy = append(eofSeenBy, sum.id)
	}

	if len(eofSeenBy) == sum.sumAmount {
		return nil
	}

	message, err := inner.SerializeEOFMessage(clientId, eofSeenBy)
	if err != nil {
		return err
	}
	return sum.inputQueue.Send(*message)
}

func (sum *Sum) flushClient(clientId uint64) error {
	slog.Info("Flushing client", "clientId", clientId)
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
