package join

import (
	"os"
	"os/signal"
	"syscall"
	"log/slog"
	"sort"

	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/fruititem"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/messageprotocol/inner"
	"github.com/7574-sistemas-distribuidos/tp-coordinacion/common/middleware"
)

type JoinConfig struct {
	MomHost           string
	MomPort           int
	InputQueue        string
	OutputQueue       string
	SumAmount         int
	SumPrefix         string
	AggregationAmount int
	AggregationPrefix string
	TopSize           int
}

type Join struct {
	inputQueue  		middleware.Middleware
	outputQueue 		middleware.Middleware
	aggregationAmount   int
	eofReceived   		map[uint64]int
	clientFruitItemMap  map[uint64]map[string]fruititem.FruitItem
	topSize             int
}

func NewJoin(config JoinConfig) (*Join, error) {
	connSettings := middleware.ConnSettings{Hostname: config.MomHost, Port: config.MomPort}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueue, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueue, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &Join{
		inputQueue: inputQueue, 
		outputQueue: outputQueue, 
		aggregationAmount: config.AggregationAmount, 
		eofReceived: make(map[uint64]int), 
		clientFruitItemMap: make(map[uint64]map[string]fruititem.FruitItem),
		topSize: config.TopSize, 
	}, nil
}

func (join *Join) Run() {
	go join.handleSignals()

	err := join.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		join.handleMessage(msg, ack, nack)
	})
	if err != nil {
		slog.Error("While consuming messages", "err", err)
	}

	join.inputQueue.Close()
	join.outputQueue.Close()
}

func (join *Join) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("SIGTERM signal received")
	join.inputQueue.StopConsuming()
}

func (join *Join) handleMessage(msg middleware.Message, ack func(), nack func()) {
	defer ack()

	clientId, fruitRecords, isEof, err := inner.DeserializeMessage(&msg)
	if err != nil {
		slog.Error("While deserializing message", "err", err)
		return
	}
	
	if !isEof {
		if _, ok := join.clientFruitItemMap[clientId]; !ok {
			join.clientFruitItemMap[clientId] = map[string]fruititem.FruitItem{}
		}
		for _, fruitRecord := range fruitRecords {
			join.clientFruitItemMap[clientId][fruitRecord.Fruit] = fruitRecord
		}
		return
	} else {
		join.eofReceived[clientId]++
		if join.eofReceived[clientId] < join.aggregationAmount {
			return
		}

		fruitTopRecords := join.buildFruitTop(clientId)
		message, err := inner.SerializeMessage(clientId, fruitTopRecords)
		if err != nil {
			slog.Error("While serializing top message", "err", err)
			return
		}
		if err := join.outputQueue.Send(*message); err != nil {
			slog.Error("While sending top message", "err", err)
			return
		}

		delete(join.eofReceived, clientId)
		delete(join.clientFruitItemMap, clientId)
	}
}

func (join *Join) buildFruitTop(clientId uint64) []fruititem.FruitItem {
	fruitItems := make([]fruititem.FruitItem, 0, len(join.clientFruitItemMap[clientId]))
	for _, item := range join.clientFruitItemMap[clientId] {
		fruitItems = append(fruitItems, item)
	}
	sort.SliceStable(fruitItems, func(i, j int) bool {
		return fruitItems[j].Less(fruitItems[i])
	})
	finalTopSize := min(join.topSize, len(fruitItems))
	return fruitItems[:finalTopSize]
}

