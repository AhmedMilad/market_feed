package main

import (
	"log"
	"market_feed/utils"

	"github.com/joho/godotenv"
)

func main() {

	err := godotenv.Load()
	if err != nil {
		log.Fatalf("Error loading .env file: %v", err)
	}

	utils.InitRedis()
	utils.GetMarketFeed()

}
