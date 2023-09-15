package main


// So if there is no state, what are our thoughts:
//
// Problem:
//  How do you handle concurrent writes?
//   2 requests writing over each other
//   Read the current value, then Compare and Swap the new swap? Retry if the compare failes
//   You could simply avoid the problem entirely, instead of tracking a sum, track individual requests with UUIDs
//   Sum all the requests on reads. This makes reads super inefficient?
//   Somehow combine these ideas?, but then you run into concurrent writes again...
//   Are concurrent writes even a thing? What does sequentially consistent even mean?


// Possilbe solution:
//  in the key value, each node tracks it's own sum separately and the sums the three values on read.
//  This seems like a good idea regardless.
//  Would it need to map to the nodes themselves? if you do compare and swap, could you just n buckets, say 20, and just pick a random one? This would mean different nodes are accessing the same buckets, which removes the benefits of sequential consistency, but if you're doing compare and swap, maybe that doesn't matter...
//  Well, each node could have it's own n buckets. Then you get both benefits
//
// So the problem with the cas approach is that the last cas is not reflected in the first two (final) reads that come after that. So it's not an over written write, it's just the last one that needs a sec. Same in the mutex solution? Would explain only having a single mistake rather than dozens...

// So it's just the final reads outrunning the final write? That's lame
// Yeah, same for the mutex solution. Does nothing but wait on the last write.
// Bizzarre, is the whole point of this problem just to bucket the sums?

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

        // Commenting out this lock always results in a single failed add. Very bizzare.
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
