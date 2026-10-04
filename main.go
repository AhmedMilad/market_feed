package main

import (
	"normalizer/utils"
	"sync"
)

func main() {

	topic := "quickstart-events"
	message := "test message"

	var wg sync.WaitGroup
	wg.Add(1)
	go utils.ConsumeMessage(topic, &wg)
	utils.PublishMessage(topic, message)

	wg.Wait()
}
