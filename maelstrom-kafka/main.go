package main

// Approaches:
//      CAS with retries
//          will degrade as node count goes up?
//      log specific writer nodes
//          each log has a single node that does all the writing to that log.
//          You can then solve concurrent requests with just mutexes
//          Would need to forward messages to the writer node. These would need to be idempotent
//              This would be the complex step
//          Supports buffering writes
//      node-specific log subsets
//          all nodes are writer nodes for all logs
//          solve contention by having each node write to its own subset of each log
//          reads would then have to correctly reconstruct a consistent log
//              This would be the complex step
//              How do you do this? Request all the log subsets, stitch them together
//              How do you make sure the log doesn't become inconsistent. You can't just sort them by timestamp
//              The other log could later add one that is before the end of a different subset
//              You could clip off anything more than the earliest ending. Then sort. That would ensure consistency
//              but it would never "converge"(?). You'd always be clipping off something
//              could you handle offsets the same way?
//       write leader

// Implemented Optimizations:
// * Have a single writer with message forwarding so you don't need to do CAS with retries
//   (messages per op, between 6.1 8.5 server)
// * Buffer writes (messages per op, between 5.5) Down a bit, but not too much.
//      Can do this because reads don't have ot be up to date, so we can tolerate some delay on writes
//      Why am I optimizing writes when it's poll reads that are killing performance. Idiot.
//      Tradeoff, latency, data loss on node crash, debugging/system anaylsis
//      Now that I implemented it, write buffers seem like a generally bad idea
// * Instant replies on buffered writes.
//      Rather than waiting for the write to be committed, just reply immediately
//      Do this by storing an in-memory hash of max offsets
//      Tradeoff, what happens when your machine crashes and you lose the in memory hash? Would need to rebuild it from the kv
// * Group logs into sets of 10 (messages per op, 3.3)
//      Reduces requests to the kv
//      Reduces latency (those requests were being made serially)
//      Tradeoff, larger messages and can cause more write contention depending on how you do everything else
//      Only works well here because we have 1 writer

// Potential optimizations
// * Cache ths logs. Read from cached logs rather than the kv
// * Somehow aggegate reads requests?
//   * bundle the logs together to minimize read requests
// * Minimize read message size? Don't return whole log for every read? You don't need it do you?
// * Biggest gains will probably be in caching read info rather then requesting whole logs.
// * Implement zipper idea. 2 datasets in the kv. Published logs for reading. Smaller write space for segmented writes paired with a periodic processing of the write space into the published logs. Works because reads don't have to be up to date
// * Keep some info locally in the node? What would I keep? Most recent logs?
//   * How does this handle if a node goes down? Get's into the idea of "promotion". Possibly out of the scope of the challenge? I think all it does is partition the nodes. But that's enough to break the segmentation idea. If you're partitioned, how do you handle that. I guess you could buffer those as well... Then your writes get *really* out of order. Again, maybe that doesn't matter. Problem only talked aout read/write consistency
// * Some kind of data retructuring
// * Making some use of multiple data structures (one for reads, one for writes?)
// * don't read before you write. Keep a local version of each log and just write what should be in the db.
//   * I mean, if we're doing that, we don't need to write either, just keep an in memory log and never use the kv. If we only have one writer..., but then who can read? forward those as well :), just use the node as the kv
//   * feels like it's going against the spirit of the problem
// * Parallelize log reads. So with the structure of the data we have, each log in it's own key. To read multiple logs in a single poll, we have to issue a bunch of reads. These are done in sequence when they should be parallelized. This would improve latency but not messages per op
//     You can do this with go routines. Look over each and go func() { }() for each one.
// * Group logs into small sets so my polls can make fewer requests vs 1 per log. Improve both latency and messages per op

// Notes:
//    There can be gaps in offsets
//    No recency requirement. Poll does not have to return the most up to date messages coming in through send
//    Poll does have to return messages without skipping any though
//    Offset log should always increase. No inserting message before previous messages. Append only


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
            for i := 0; i <= (500/groupSize); i++ {
                writeLogCtx, writeLogCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                writeOffsetCtx, writeOffsetCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                defer writeLogCancel()
                defer writeOffsetCancel()

                defaultLogs := map[string][]int{}
                for j := 0; j < groupSize; j++ {
                    defaultLogs[strconv.Itoa(j+i*10)] = []int{}
                }
                kLogs.Write(writeLogCtx, "logGroup-" + strconv.Itoa(i*10), defaultLogs)

                defaultOffsets := map[string]int{}
                for j := 0; j < groupSize; j++ {
                    defaultOffsets[strconv.Itoa(j+i*10)] = 0
                }
                kLogs.Write(writeOffsetCtx, "offsetGroup-" + strconv.Itoa(i*10), defaultOffsets)
            }
        }

        return n.Reply(msg, map[string]interface{}{ "type": "init_ok" })
    })

    // Test removing this. I don't think it did anything regarding the client crashes
    n.Handle("topology", func(msg maelstrom.Message) error {
        return n.Reply(msg, map[string]interface{}{ "type": "topology_ok" })
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
    // n.Handle("send", func(msg maelstrom.Message) error {
    //     return forwardAndRespond(
    //         n,
    //         writerNodeID,
    //         msg,
    //         func(body map[string]interface{}) (map[string]interface{}, error) {
    //             logKey := body["key"].(string)
    //             logMessage := int(body["msg"].(float64))
    //
    //             // Locking across a network request. Feels wrong. Would channels do any better?
    //             // No... CAS likely only superior solution
    //             // Do I need to do this? Yes? That's the whole point?
    //             kLogsMutex.Lock()
    //
    //             // Read the previous value of the log
    //             readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
    //             defer readCancel()
    //             kLog, readErr := kLogs.Read(readCtx, "log-"+logKey)
    //             if readErr != nil {
    //                 return nil, readErr
    //             }
    //
    //             // Append the new value
    //             newKLog := append(kLog.([]interface{}), logMessage)
    //
    //             // Write the new log back to the kv
    //             writeCtx, writeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
    //             defer writeCancel()
    //             kLogs.Write(writeCtx, "log-"+logKey, newKLog)
    //
    //             kLogsMutex.Unlock()
    //
    //             return map[string]interface{}{
    //                 "type": "send_ok",
    //                 "offset": len(newKLog) - 1,
    //             }, nil
    //         },
    //     )
    // })

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

                for groupKey, _ := range groupKeys {
                    kLogCommittedOffsetsMutex.Lock()

                    // Read the group
                    readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                    defer readCancel()
                    groupKeyString := strconv.Itoa(groupKey)
                    kLogCommittedOffsetGroupTemp, readErr := kLogs.Read(readCtx, "offsetGroup-" + groupKeyString)
                    if readErr != nil {
                        return nil, readErr
                    }
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
                }

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
        for groupKey, _ := range groupKeys {
            // Read the group
            readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
            defer readCancel()
            groupKeyString := strconv.Itoa(groupKey)
            kLogGroupTemp, readErr := kLogs.Read(readCtx, "logGroup-" + groupKeyString)
            if readErr != nil {
                return readErr
            }
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
                    offsetResponses[logKey] = offsetMessages
                }
            }
        }

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
        for groupKey, _ := range groupKeys {
            // Read the group
            readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
            defer readCancel()
            groupKeyString := strconv.Itoa(groupKey)
            kLogCommittedOffsetGroupTemp, readErr := kLogs.Read(readCtx, "offsetGroup-" + groupKeyString)
            if readErr != nil {
                return readErr
            }
            kLogCommittedOffsetGroup := kLogCommittedOffsetGroupTemp.(map[string]interface{})

            // Extract the relevant offsets
            for _, requestedOffsetKey := range requestedOffsetKeys {
                requestedOffsetKeyInt, _ := strconv.Atoi(requestedOffsetKey.(string))
                if requestedOffsetKeyInt / 10 * 10 == groupKey {
                    offset := kLogCommittedOffsetGroup[requestedOffsetKey.(string)]
                    responseOffsets[requestedOffsetKey.(string)] = int(offset.(float64))
                }
            }
        }

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

                for groupKey, _ := range groupKeys {
                    kLogsMutex.Lock() // Don't think this is necessary anymore since this has been moved to the ticker

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
                }
            }
        }
    }()

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
