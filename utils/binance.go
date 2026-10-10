package utils

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"context"
	"time"

	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"
	"net"
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

type SymbolResp struct {
	Symbol string `json:"symbol"`
}

func GetMarketFeed() {

	for {
		err := runMarketFeed()

		if err != nil {
			log.Printf("Market feed disconnected : %v", err)
		}

		log.Printf("Reconnecting in 5 seconds.")

		time.Sleep(5 * time.Second)
	}

}

func runMarketFeed() error {
	url := fmt.Sprintf("%v/stream", os.Getenv("BINANCE_WSS_URL"))

	const timeout = 60 * time.Second
	const waitTime = 10 * time.Second

	conn, _, err := websocket.DefaultDialer.Dial(url, http.Header{})
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer conn.Close()

	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return fmt.Errorf("Failed to set initial read deadline: %w", err)
	}

	conn.SetPingHandler(func(appData string) error {
		if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
			return err
		}

		return conn.WriteControl(
			websocket.PongMessage,
			[]byte(appData),
			time.Now().Add(waitTime),
		)
	})

	log.Println("Connected!")

	subscription := SubscribeMessage{
		Method: "SUBSCRIBE",
		Params: []string{
			"bnbusdt@aggTrade",
			"btcusdt@aggTrade",
		},
	}

	// avoid write blocks
	if err := conn.SetWriteDeadline(time.Now().Add(waitTime)); err != nil {
		return fmt.Errorf("failed to set subscription write deadline: %w", err)
	}

	if err := conn.WriteJSON(subscription); err != nil {
		return fmt.Errorf("failed to send subscription: %w", err)
	}

	log.Println("Subscription sent!")

	for {
		messageType, message, err := conn.ReadMessage()

		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return fmt.Errorf("WebSocket read timed out: %w", err)
			}
			return fmt.Errorf("WebSocket read failed: %w", err)

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
		err = PublishMessage(os.Getenv("AGG_FEED_TOPIC"), os.Getenv("AGG_FEED_KEY"), jsonData)

		if err != nil {
			return err
		}
	}
}

func UpdateSymbolList() error {
	url := fmt.Sprintf("%v/v3/ticker/price", os.Getenv("BINANCE_API_URL"))

	client := &http.Client{
		Timeout: 10 * time.Second,
	}

	resp, err := client.Get(url)
	if err != nil {
		return fmt.Errorf("could not request Binance symbols: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("Binance returned HTTP status: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("could not read response body: %w", err)
	}

	var symbols []SymbolResp
	if err := json.Unmarshal(body, &symbols); err != nil {
		return fmt.Errorf("could not parse Binance symbols: %w", err)
	}

	ctx := context.Background()

	if err := RDB.Set(ctx, "symbols", body, 30*time.Minute).Err(); err != nil {
		return fmt.Errorf("could not cache symbols in Redis: %w", err)
	}

	log.Printf("Successfully cached %d symbols", len(symbols))

	return nil
}

func GetSymbolList(tries int) ([]SymbolResp, error) {

	if tries <= 0 {

		return nil, errors.New("Could not fetch the symbols.")

	}

	ctx := context.Background()
	key := "symbols"

	body, err := RDB.Get(ctx, key).Bytes()

	if errors.Is(err, redis.Nil) {
		if err := UpdateSymbolList(); err != nil {
			return nil, err
		}

		return GetSymbolList(tries - 1)
	}

	if err != nil {
		return nil, err
	}

	var resp []SymbolResp

	if err := json.Unmarshal(body, &resp); err != nil {

		return nil, fmt.Errorf("could not parse cached symbols: %w", err)
	}

	return resp, nil
}
