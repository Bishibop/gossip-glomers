package main

// Bucket the sums in the KV and sum those buckets on read.
// This removes internode write contention.
// Contention can only come from within a single node across multiple requests. Solved with mutexs or CAS.

import (
    "log"
    "os"
    // "github.com/google/uuid"
    "encoding/json"
    // "reflect"
    // "fmt"
    "sync"
    // "math/rand"
    "time"
    "context"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    kv := maelstrom.NewSeqKV(n)
    kvMutex := &sync.Mutex{}

    // Simple mutex locking
    // This works at a rate of 500
    n.Handle("add", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        delta := int(body["delta"].(float64))

        readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
        defer readCancel()

        // Commenting out this lock always results in a single failed add at the very end. Bizarre.
        // Feels like it should be a lot more? Maybe a simultaneous write is hardcoded in the workload.
        kvMutex.Lock()

        value, readErr := kv.Read(readCtx, n.ID())
        if readErr != nil {
            if maelstrom.ErrorCode(readErr) == 20 {
                value = 0
            } else {
                return readErr
            }
        }

        writeCtx, writeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
        defer writeCancel()

        writeErr := kv.Write(writeCtx, n.ID(), value.(int) + delta)
        if writeErr != nil {
            return writeErr
        }

        kvMutex.Unlock()

        response := map[string]interface{}{
            "type": "add_ok",
        }
        return n.Reply(msg, response)
    })

    // CompareAndSwap
    // n.Handle("add", func(msg maelstrom.Message) error {
    //     var body map[string]interface{}
    //     if err := json.Unmarshal(msg.Body, &body); err != nil {
    //         return err
    //     }
    //
    //     delta := int(body["delta"].(float64))
    //
    //     readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
    //     defer readCancel()
    //     value, readErr := kv.Read(readCtx, n.ID())
    //     if readErr != nil {
    //         if maelstrom.ErrorCode(readErr) == 20 {
    //             value = 0
    //         } else {
    //             return readErr
    //         }
    //     }
    //
    //     rand.Seed(time.Now().UnixNano())
    //     retries := 5
    //     for i := 0; i < retries; i++ {
    //         casCtx, casCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
    //         defer casCancel()
    //         // I wonder if this can still have concurrent successful writes that overwrite each other?
    //         casErr := kv.CompareAndSwap(casCtx, n.ID(), value.(int), value.(int) + delta, true)
    //         if casErr == nil {
    //             break
    //         } else if i < retries-1 {
    //             // This never get's hit, even in the failure cases. Simply never returns an error...
    //             sleepDuration := time.Duration(rand.Intn(i+1)) * time.Second
    //             time.Sleep(sleepDuration)
    //         } else {
    //             return casErr
    //         }
    //     }
    //
    //     response := map[string]interface{}{
    //         "type": "add_ok",
    //     }
    //     return n.Reply(msg, response)
    // })

    n.Handle("read", func(msg maelstrom.Message) error {
        sum := 0
        for _, nodeID := range n.NodeIDs() {
            ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
            defer cancel()
            value, err := kv.Read(ctx, nodeID)
            if err == nil {
                sum += value.(int)
            } else if maelstrom.ErrorCode(err) == 20 {
                // pass without error, sum is already 0
            } else {
                return err
            }
        }

        response := map[string]interface{}{
            "type": "read_ok",
            "value": sum,
        }
        return n.Reply(msg, response)
    })

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
