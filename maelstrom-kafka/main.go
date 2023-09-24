package main

// Challenge in this problem: Write contention. Multiple nodes trying to write to the same log

// Main thing I learned: How you structure data in your shared resource defines everything about how your system works

// Implemented Approach: Single write leader with message forwarding
// Tradeoffs:
// * Solves write contention with a simple solution
// * Doesn't increase messages-per-op too much
// * Requires a single writer node. Strict bottleneck to scaling the system

// Implemented Optimizations:
// * Have a single writer with message forwarding so you don't need to do CAS with retries
//   (messages per op, between 6.1)
// * Buffer writes (messages per op, 5.5) Down a bit, but not too much.
//      Can do this because reads don't have ot be up to date, so we can tolerate some delay on writes
//      Why am I optimizing writes when it's poll reads that are killing performance?
//      Drawbacks: latency, data loss on node crash, debugging/system anaylsis
//      Now that I implemented it, write buffers seem like a generally bad idea
// * Instant replies on buffered writes.
//      Rather than waiting for the write to be committed, just reply immediately
//      Do this by storing an in-memory hash of max offsets
//      Tradeoff, what happens when your machine crashes and you lose the in memory hash? Would need to rebuild it from the kv
// * Group logs into sets of 10 (messages per op, 3.3)
//      Significantly reduces requests to the kv
//      Tradeoff, larger messages and can cause more write contention depending on how you do everything else
//      Only works well here because we have 1 writer so there's no contention. Otherwise, it would massively increase the amount of data being access simultaneously
// * Parallelize requests to the KV with goroutines. Messages per opp stay the same but latency and lag go down.


// Notes:

// Shape of the problem
//    There can be gaps in offsets
//    No recency requirement. Poll does not have to return the most up to date messages coming in through send
//    Poll does have to return messages without skipping any though
//    Offset log should always increase. No inserting message before previous messages. Append only
//
// Possible Approaches:
//      CAS with retries
//          will degrade as node count goes up?
//          expensive in terms of messages per op
//      Single write leader with message forwarding
//      Log specific writer nodes with message forwarding
//          each log has a single node that does all the writing to that log.
//          You can then solve concurrent requests with just mutexes
//          Would need to forward messages to the writer node. These would need to be idempotent
//              This would be the complex step
//          Supports buffering writes
//      Node-specific log subsets (probably the best solution)
//          all nodes write to all logs without contention (no message forwarding)
//          can scale arbitrarily
//          Key insight: separate read and write spaces in the KV
//          Each node writes to it's own subset of each log. These are then periodically stitched together into a separate, single log for reading

// Potential optimizations
// * Cache ths logs. Read from cached logs rather than the kv
// * Replace read-write locks with CAS with retries
//      Would work very well with a single writer node.
//      Only contention would be within writer goroutines, not between nodes
//      Should improve latency and lag
// * Minimize read message size? Don't return whole log for every read?
// * Implement zipper idea. 2 datasets in the kv. Published logs for reading. Smaller write space for segmented writes paired with a periodic processing of the write space into the published logs. Works because reads don't have to be up to date. Allows all nodes to write without contention and can scale up additional nodes. No single node bottleneck. Probably best solution.
// * Keep some info locally in the node? What would I keep? Most recent logs?
//   * How does this handle if a node goes down? Get's into the idea of "promotion". Possibly out of the scope of the challenge? I think all it does is partition the nodes. But that's enough to break the segmentation idea. If you're partitioned, how do you handle that. I guess you could buffer those as well... Then your writes get *really* out of order. Again, maybe that doesn't matter. Problem only talked aout read/write consistency
// * Some kind of data retructuring
// * Making some use of multiple data structures (one for reads, one for writes?)
// * don't read before you write. Keep a local version of each log and just write what should be in the db.
//   * I mean, if we're doing that, we don't need to write either, just keep an in memory log and never use the kv. If we only have one writer..., but then who can read? forward those as well :), just use the node as the kv
//   * feels like it's going against the spirit of the problem



import (
    "log"
    "os"
    // "github.com/google/uuid"
    "encoding/json"
    // "reflect"
    // "fmt"
    "strconv"
    // "strings"
    "sync"
    // "math/rand"
    "time"
    "context"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func forwardAndRespond(node *maelstrom.Node, targetNodeID string, msg maelstrom.Message, buildResponse func(map[string]interface{}) (map[string]interface{}, error)) error {
    var body map[string]interface{}
    if err := json.Unmarshal(msg.Body, &body); err != nil {
        return err
    }

    if node.ID() == targetNodeID {
        response, err := buildResponse(body)
        if err != nil {
            return err
        }
        // If this was a forwarded messages
        if body["originalSenderID"] != nil {
            // Respond to the original (external) sender
            response["in_reply_to"] = body["originalMessageID"]
            return node.Send(body["originalSenderID"].(string), response)
            // Respond to the fowarding node
            // Only need this for idempotency if I decide to reimplement it
            // return n.Reply(msg, map[string]interface{}{ "type": "send_ok" })
        } else {
            return node.Reply(msg, response)
        }
    } else {
        // Forward to the appropriate writer node
        body["originalSenderID"] = msg.Src
        body["originalMessageID"] = body["msg_id"]
        return node.Send(targetNodeID, body)
    }
}

func main() {
    n := maelstrom.NewNode()
    writerNodeID := "n1"
    groupSize := 10
    kLogs := maelstrom.NewLinKV(n)
    kLogsMutex := &sync.Mutex{}
    kLogCommittedOffsetsMutex := &sync.Mutex{}
    writeBuffer := map[string][]interface{}{}
    writeBufferMutex := &sync.Mutex{}
    latestOffsets := [500]int{}
    latestOffsetsMutex := &sync.Mutex{}

    // Initialize the logs
    n.Handle("init", func(msg maelstrom.Message) error {
        if n.ID() == writerNodeID {
            var wg sync.WaitGroup
            for i := 0; i <= (500/groupSize); i++ {
                wg.Add(1)
                go func(k int) {
                    defer wg.Done()
                    writeLogCtx, writeLogCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                    writeOffsetCtx, writeOffsetCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                    defer writeLogCancel()
                    defer writeOffsetCancel()

                    defaultLogs := map[string][]int{}
                    for j := 0; j < groupSize; j++ {
                        defaultLogs[strconv.Itoa(j+k*10)] = []int{}
                    }
                    kLogs.Write(writeLogCtx, "logGroup-" + strconv.Itoa(k*10), defaultLogs)

                    defaultOffsets := map[string]int{}
                    for j := 0; j < groupSize; j++ {
                        defaultOffsets[strconv.Itoa(j+k*10)] = 0
                    }
                    kLogs.Write(writeOffsetCtx, "offsetGroup-" + strconv.Itoa(k*10), defaultOffsets)
                }(i)
            }
            wg.Wait()
        }

        // Replying to "init" is causing crashes because the node c0 is crashing and can't receive the message
        // return n.Reply(msg, map[string]interface{}{ "type": "init_ok" })
        return nil
    })

    // Request
    // {
    //   "type": "send",
    //   "key": "k1",
    //   "msg": 123
    // }
    // Response
    // {
    //   "type": "send_ok",
    //   "offset": 1000
    // }
    n.Handle("send", func(msg maelstrom.Message) error {
        return forwardAndRespond(
            n,
            writerNodeID,
            msg,
            func(body map[string]interface{}) (map[string]interface{}, error) {
                logKey := body["key"].(string)
                logMessage := body["msg"]

                writeBufferMutex.Lock()
                writeBuffer[logKey] = append(writeBuffer[logKey], logMessage)
                writeBufferMutex.Unlock()

                i, _ := strconv.Atoi(logKey)
                latestOffsetsMutex.Lock()
                latestOffsets[i] = latestOffsets[i] + 1
                latestOffsetsMutex.Unlock()

                return map[string]interface{}{
                    "type": "send_ok",
                    "offset": latestOffsets[i] - 1,
                }, nil
            },
        )
    })

    // Request
    // {
    //   "type": "commit_offsets",
    //   "offsets": {
    //     "k1": 1000,
    //     "k2": 2000
    //   }
    // }
    // Response
    // {
    //   "type": "commit_offsets_ok"
    // }
    n.Handle("commit_offsets", func(msg maelstrom.Message) error {
        return forwardAndRespond(
            n,
            writerNodeID,
            msg,
            func(body map[string]interface{}) (map[string]interface{}, error) {
                committedOffsets := body["offsets"].(map[string]interface{})

                // Identify the relevant groups
                groupKeys := map[int]interface{}{}
                for logKey, _ := range committedOffsets {
                    logKeyInt, _ := strconv.Atoi(logKey)
                    groupKeys[logKeyInt / 10 * 10] = nil
                }

                wg := sync.WaitGroup{}
                for groupKey, _ := range groupKeys {
                    wg.Add(1)
                    go func(groupKey int) {
                        defer wg.Done()
                        kLogCommittedOffsetsMutex.Lock()

                        // Read the group
                        readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                        defer readCancel()
                        groupKeyString := strconv.Itoa(groupKey)
                        kLogCommittedOffsetGroupTemp, _ := kLogs.Read(readCtx, "offsetGroup-" + groupKeyString)
                        kLogCommittedOffsetGroup := kLogCommittedOffsetGroupTemp.(map[string]interface{})

                        // Update the relevant offsets
                        for logKey, proposedOffset := range committedOffsets {
                            logKeyInt, _ := strconv.Atoi(logKey)
                            if logKeyInt / 10 * 10 == groupKey {
                                offset := int(proposedOffset.(float64))
                                if offset >= int(kLogCommittedOffsetGroup[logKey].(float64)) {
                                    kLogCommittedOffsetGroup[logKey] = offset
                                }
                            }
                        }

                        // Write the group
                        writeCtx, writeCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                        defer writeCancel()
                        kLogs.Write(writeCtx, "offsetGroup-" + groupKeyString, kLogCommittedOffsetGroup)

                        kLogCommittedOffsetsMutex.Unlock()
                    }(groupKey)
                }
                wg.Wait()

                return map[string]interface{}{
                    "type": "commit_offsets_ok",
                }, nil
            },
        )
    })

    // Request
    // {
    //   "type": "poll",
    //   "offsets": {
    //     "k1": 1000,
    //     "k2": 2000
    //   }
    // }
    // Response
    // {
    //   "type": "poll_ok",
    //   "msgs": {
    //     "k1": [[1000, 9], [1001, 5], [1002, 15]],
    //     "k2": [[2000, 7], [2001, 2]]
    //   }
    // }
    n.Handle("poll", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        requestedOffsets := body["offsets"].(map[string]interface{})

        // Identify the relevant groups
        groupKeys := map[int]interface{}{}
        for logKey, _ := range requestedOffsets {
            logKeyInt, _ := strconv.Atoi(logKey)
            groupKeys[logKeyInt / 10 * 10] = nil
        }

        offsetResponses := map[string][][]int{}
        // offsetResponsesMutex := sync.Mutex{}
        // wg := sync.WaitGroup{}
        for groupKey, _ := range groupKeys {
            // wg.Add(1)
            // go func(groupKey int) {
                // defer wg.Done()
                // Read the group
                readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                defer readCancel()
                groupKeyString := strconv.Itoa(groupKey)
                kLogGroupTemp, _ := kLogs.Read(readCtx, "logGroup-" + groupKeyString)
                kLogGroup := kLogGroupTemp.(map[string]interface{})

                // Extract the relevant offsets
                for logKey, requestedOffset := range requestedOffsets {
                    logKeyInt, _ := strconv.Atoi(logKey)
                    if logKeyInt / 10 * 10 == groupKey {
                        offset := int(requestedOffset.(float64))
                        offsetMessages := [][]int{}
                        kLog, ok := kLogGroup[logKey].([]interface{})
                        if !ok {
                            kLog = []interface{}{}
                        }
                        var kLogOffsetSlice []interface{}
                        if len(kLog) >= offset + 100 {
                            kLogOffsetSlice = kLog[offset:offset+100]
                        } else {
                            kLogOffsetSlice = kLog[offset:]
                        }
                        for i, logMessage := range kLogOffsetSlice {
                            offsetMessages = append(offsetMessages, []int{offset + i, int(logMessage.(float64))})
                        }
                        // offsetResponsesMutex.Lock()
                        offsetResponses[logKey] = offsetMessages
                        // offsetResponsesMutex.Unlock()
                    }
                }
            // }(groupKey)
        }
        // wg.Wait()

        response := map[string]interface{}{
            "type": "poll_ok",
            "msgs": offsetResponses,
        }
        return n.Reply(msg, response)
    })

    // Request
    // {
    //   "type": "list_committed_offsets",
    //   "keys": ["k1", "k2"]
    // }
    // Response
    // {
    //   "type": "list_committed_offsets_ok",
    //   "offsets": {
    //     "k1": 1000,
    //     "k2": 2000
    //   }
    // }
    n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        requestedOffsetKeys := body["keys"].([]interface{})

        // Identify the relevant groups
        groupKeys := map[int]interface{}{}
        for _, logKey := range requestedOffsetKeys {
            logKeyInt, _ := strconv.Atoi(logKey.(string))
            groupKeys[logKeyInt / 10 * 10] = nil
        }

        responseOffsets := map[string]int{}
        responseOffsetsMutex := sync.Mutex{}
        wg := sync.WaitGroup{}
        for groupKey, _ := range groupKeys {
            wg.Add(1)
            go func(groupKey int) {
                defer wg.Done()
                // Read the group
                readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                defer readCancel()
                groupKeyString := strconv.Itoa(groupKey)
                kLogCommittedOffsetGroupTemp, _ := kLogs.Read(readCtx, "offsetGroup-" + groupKeyString)
                kLogCommittedOffsetGroup := kLogCommittedOffsetGroupTemp.(map[string]interface{})

                // Extract the relevant offsets
                for _, requestedOffsetKey := range requestedOffsetKeys {
                    requestedOffsetKeyInt, _ := strconv.Atoi(requestedOffsetKey.(string))
                    if requestedOffsetKeyInt / 10 * 10 == groupKey {
                        offset := kLogCommittedOffsetGroup[requestedOffsetKey.(string)]
                        responseOffsetsMutex.Lock()
                        responseOffsets[requestedOffsetKey.(string)] = int(offset.(float64))
                        responseOffsetsMutex.Unlock()
                    }
                }
            }(groupKey)
        }
        wg.Wait()

        response := map[string]interface{}{
            "type": "list_committed_offsets_ok",
            "offsets": responseOffsets,
        }
        return n.Reply(msg, response)
    })

    go func() {
        writeBufferFlushTicker := time.NewTicker(time.Second / 50)
        defer writeBufferFlushTicker.Stop()
        for range writeBufferFlushTicker.C {
            if len(writeBuffer) > 0 {
                writeBufferMutex.Lock()
                tempWriteBuffer := writeBuffer
                writeBuffer = map[string][]interface{}{}
                writeBufferMutex.Unlock()

                // Identify the relevant groups
                groupKeys := map[int]interface{}{}
                for logKey, _ := range tempWriteBuffer {
                    logKeyInt, _ := strconv.Atoi(logKey)
                    groupKeys[logKeyInt / 10 * 10] = nil
                }

                wg := sync.WaitGroup{}
                for groupKey, _ := range groupKeys {
                    wg.Add(1)
                    go func(groupKey int) {
                        defer wg.Done()
                        kLogsMutex.Lock()

                        // Read the group
                        readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                        defer readCancel()
                        groupKeyString := strconv.Itoa(groupKey)
                        kLogGroupTemp, _ := kLogs.Read(readCtx, "logGroup-" + groupKeyString)
                        kLogGroup, ok := kLogGroupTemp.(map[string]interface{})
                        if !ok {
                            kLogGroup = map[string]interface{}{}
                        }

                        // Append the messages to each log
                        for logKey, bufferedMessages := range tempWriteBuffer {
                            logKeyInt, _ := strconv.Atoi(logKey)
                            if logKeyInt / 10 * 10 == groupKey {
                                currentLog, ok := kLogGroup[logKey].([]interface{})
                                if !ok {
                                    currentLog = []interface{}{}
                                }
                                kLogGroup[logKey] = append(currentLog, bufferedMessages...)
                            }
                        }

                        // Write the group
                        writeCtx, writeCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                        defer writeCancel()
                        kLogs.Write(writeCtx, "logGroup-" + groupKeyString, kLogGroup)

                        kLogsMutex.Unlock()
                    }(groupKey)
                }
                wg.Wait()
            }
        }
    }()

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
