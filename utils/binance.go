package utils

import (
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
		Params: []string{"bnbusdt@aggTrade"},
	}

	err = conn.WriteJSON(subscription)
	if err != nil {
		log.Fatal("failed to send subscription:", err)
	}

	log.Println("Subscription sent!")

	for {
		messageType, message, err := conn.ReadMessage()
		if err != nil {
			log.Println("connection closed:", err)
			break
		}

		switch messageType {
		case websocket.TextMessage:
			log.Printf("Received: %s\n", message)

		case websocket.BinaryMessage:
			log.Printf("Received binary message: %d bytes\n", len(message))
		}
	}
}
