/*
Notes:
    * I feel like nemisis isn't partitioning the network.
    * This has no forwarding buffer or retries. It shouldn't pass
    * Yeah, why does this pass? If there is a partitiion, the writes never get sent to the other nodes.
    * Does that not matter? Maybe I don't have a good understanding of the consistency model.
*/

package main

import (
    "log"
    "os"
    // "github.com/google/uuid"
    "encoding/json"
    // "reflect"
    // "fmt"
    // "strconv"
    // "strings"
    "sync"
    // "math/rand"
    // "time"
    // "context"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
    n := maelstrom.NewNode()
    kv := map[int][]int{}
    kvMutex := sync.Mutex{}

    n.Handle("init", func(msg maelstrom.Message) error {
        return n.Reply(msg, map[string]interface{}{ "type": "init_ok" })
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        return n.Reply(msg, map[string]interface{}{ "type": "topology_ok" })
    })

    // Request:
    // {
    //   "type": "txn",
    //   "msg_id": 3,
    //   "txn": [
    //     ["r", 1, null],
    //     ["w", 1, 6],
    //     ["w", 2, 9]
    //   ]
    // }
    // Response:
    // {
    //   "type": "txn_ok",
    //   "msg_id": 1,
    //   "in_reply_to": 3,
    //   "txn": [
    //     ["r", 1, 3],
    //     ["w", 1, 6],
    //     ["w", 2, 9]
    //   ]
    // }
    n.Handle("txn", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        txn := body["txn"].([]interface{})
        writeTxn := []interface{}{}

        kvMutex.Lock()
        for _, op := range txn {
            op := op.([]interface{})
            switch op[0] {
            case "r":
                key := int(op[1].(float64))
                val := 0
                tuple, ok := kv[key]
                if ok {
                    val = tuple[0]
                }
                op[2] = val
            case "w":
                key := int(op[1].(float64))
                val := int(op[2].(float64))
                kv[key] = []int{val, int(body["msg_id"].(float64))}

                writeTxn = append(writeTxn, op)
            }
        }
        kvMutex.Unlock()

        // Don't reforward forwarded messages
        // If this problem had more than 2 nodes, we would need an actual broadcasting algorithm here
        forwarded, _ := body["forwarded"].(bool)
        if forwarded {
            return nil
        } else {
            body["forwarded"] = true
            body["txn"] = writeTxn
            if n.ID() == "n1" {
                n.Send("n0", body)
            } else {
                n.Send("n1", body)
            }

            return n.Reply(msg, map[string]interface{}{
                "type": "txn_ok",
                "txn": txn,
            })
        }
    })

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
