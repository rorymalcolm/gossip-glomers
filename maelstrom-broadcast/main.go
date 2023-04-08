package main

import (
	"encoding/json"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
	zap "go.uber.org/zap"
)

func main() {
	n := maelstrom.NewNode()
	logger, err := zap.NewProduction()
	if err != nil {
		log.Fatal(err)
	}
	/*
	we receive messages like this:

	{
		"type": "broadcast",
		"message": 1000
	}

	we want to store the message in a map
	*/

	message_map := make(map[int]int)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		var return_body map[string]any = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		logger.Info("Received broadcast", zap.Any("body", body))
		message_body_f64 := body["message"].(float64)
		message_body_int := int(message_body_f64)
		message_map[message_body_int] = message_body_int
		return_body["type"] = "broadcast_ok"
		return n.Reply(msg, return_body)
	})
	
	n.Handle("read", func(msg maelstrom.Message) error {
		var body map[string]any
		var return_body map[string]any = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return_body["type"] = "read_ok"
		var message_list []int
		for _, v := range message_map {
			message_list = append(message_list, v)
		}
		return_body["messages"] = message_list
		return n.Reply(msg, return_body)
	})

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		var return_body map[string]any = make(map[string]any)
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}
		return_body["type"] = "topology_ok"
		return n.Reply(msg, return_body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}

}
