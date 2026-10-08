package utils

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/gorilla/websocket"
)

type SubscribeMessage struct {
	Method string   `json:"method"`
	Params []string `json:"params"`
}

type Data struct {
	EventType   string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	TradeID     int64  `json:"a"`
	Price       string `json:"p"`
	Quantity    string `json:"q"`
	TradeTime   int64  `json:"T"`
	IsBuyerMake bool   `json:"m"`
}

type AggFeed struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`
}

type AggResponse struct {
	Symbol       string `json:"symbol"`
	TradeID      int64  `json:"trade_id"`
	Price        string `json:"price"`
	Quantity     string `json:"quantity"`
	TradeTime    int64  `json:"trade_time"`
	IsBuyerMaker bool   `json:"is_buyer_maker"`
	EventTime    int64  `json:"event_time"`
}

func GetMarketFeed() {
	url := fmt.Sprintf("%v/stream", os.Getenv("BINANCE_WSS_URL"))

	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{})
	if err != nil {
		log.Fatal("failed to connect:", err)
	}
	defer conn.Close()

	log.Println("Connected!")

	subscription := SubscribeMessage{
		Method: "SUBSCRIBE",
		Params: []string{
			"bnbusdt@aggTrade",
			"btcusdt@aggTrade",
		},
	}

	if err := conn.WriteJSON(subscription); err != nil {
		log.Fatal("failed to send subscription:", err)
	}

	log.Println("Subscription sent!")

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("connection closed:", err)
			break
		}

		if messageType != websocket.TextMessage {
			log.Printf("Received non-text message: %d bytes\n", len(message))
			continue
		}

		var env AggFeed
		if err := json.Unmarshal(message, &env); err != nil {
			log.Printf("Error parsing envelope: %v | raw: %s", err, message)
			continue
		}

		if len(env.Data) == 0 {
			log.Printf("Control message: %s", message)
			continue
		}

		var d Data
		if err := json.Unmarshal(env.Data, &d); err != nil {
			log.Printf("Error parsing data: %v | raw: %s", err, message)
			continue
		}

		if d.EventType != "aggTrade" {
			continue
		}

		aggResp := AggResponse{
			Symbol:       d.Symbol,
			TradeID:      d.TradeID,
			Price:        d.Price,
			Quantity:     d.Quantity,
			TradeTime:    d.TradeTime,
			IsBuyerMaker: d.IsBuyerMake,
			EventTime:    d.EventTime,
		}

		jsonData, err := json.Marshal(aggResp)
		if err != nil {
			log.Printf("Error marshalling: %v", err)
			continue
		}

		// publish event to kafka topic.
		PublishMessage(os.Getenv("AGG_FEED_TOPIC"), os.Getenv("AGG_FEED_KEY"), jsonData)
	}
}
