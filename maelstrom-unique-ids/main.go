package main

import (
    "log"
    "os"
    // "github.com/google/uuid"
    "fmt"
    "strconv"

    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

// Interleaved Partitions
// Each node uses its id to generate incrementing ids that never intersect with other nodes.
// This property only holds if the number of nodes is fixed.
func main() {
    n := maelstrom.NewNode()
    counter := 0

    n.Handle("generate", func(msg maelstrom.Message) error {
        numberOfNodes := len(n.NodeIDs())
        partitionSeed, err := strconv.Atoi(n.ID()[1:])
        if err != nil {
            fmt.Println("Error converting string to integer:", err)
            return err
        }

        body := map[string]interface{}{}
        body["type"] = "generate_ok"
        body["id"] = partitionSeed + (counter * numberOfNodes)

        counter = counter + 1

        return n.Reply(msg, body)
    })

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}


// Timestamps
// Wouldn't work in actual distributed system? Different machines have small purturbations in their clocks.
// Would cause collisions if throughput was high enough.

// n.Handle("generate", func(msg maelstrom.Message) error {
//     body := map[string]interface{}{}
//     body["type"] = "generate_ok"
//     body["id"] = time.Now().String()
//
//     return n.Reply(msg, body)
// })


// UUID generation

// n.Handle("generate", func(msg maelstrom.Message) error {
//     id, err := uuid.NewRandom()
//     if err != nil {
//         fmt.Println("Error generating UUID:", err)
//     }
//
//     body := map[string]interface{}{}
//     body["type"] = "generate_ok"
//     body["id"] = id.String()
//
//     return n.Reply(msg, body)
// })
