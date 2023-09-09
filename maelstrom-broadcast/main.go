package main

import (
    "log"
    "os"
    // "github.com/google/uuid"
    "encoding/json"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    messages := map[float64]struct{}{}

    // {
    //   "type": "broadcast",
    //   "message": 1000
    // }
    // {
    //   "type": "broadcast_ok"
    // }
    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        messages[body["message"].(float64)] = struct{}{}

        response := map[string]interface{}{}
        response["type"] = "broadcast_ok"

        return n.Reply(msg, response)
    })

    // {
    //   "type": "read"
    // }
    // {

    //   "type": "read_ok",
    //   "messages": [1, 8, 72, 25]
    // }
    n.Handle("read", func(msg maelstrom.Message) error {
        response := map[string]interface{}{}
        response["type"] = "read_ok"
        messageKeys := make([]float64, 0, len(messages))
        for key := range messages {
          messageKeys = append(messageKeys, key)
        }
        response["messages"] = messageKeys

        return n.Reply(msg, response)
    })

    // {
    //   "type": "topology",
    //   "topology": {
    //     "n1": ["n2", "n3"],
    //     "n2": ["n1"],
    //     "n3": ["n1"]
    //   }
    // }
    // {
    //   "type": "topology_ok"
    // }
    n.Handle("topology", func(msg maelstrom.Message) error {
        response := map[string]interface{}{}
        response["type"] = "topology_ok"

        return n.Reply(msg, response)
    })

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
