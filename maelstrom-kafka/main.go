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
//      Seems like it killed latency. Realtime lag went from like .3 to 9 seconds
//      Why am I optimizing writes when it's poll reads that are killing performance. Idiot.
//      Now that I think about it, write buffers seem like generally bad idea

// Potential optimizations
// * Buffer writes
//   * You can do this because read's don't have to be up to date, so it doesn't matter if writes are delayed
//   * Does risk losing the buffer on machine failure
// * Instant replies on buffered writes.
//   * Rather than waiting for the write to be committed, just reply immediately
//   * Would need to store a hash of offsets to reply to "send" messages
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
    kLogs := maelstrom.NewLinKV(n)
    kLogsMutex := &sync.Mutex{}
    kLogCommittedOffsetsMutex := &sync.Mutex{}
    // {
    //   send-$LOG_KEY: [logMsg, logMsg, logMsg],
    //   send-$LOG_KEY: [logMsg, logMsg, logMsg],
    //   commitOffsets-$LOG_KEY: latestOffset,
    //   commitOffsets-$LOG_KEY: latestOffset,
    // }
    // We are appending to the send array value and replacing the offsets values
    // Now, what interactions are there here with other messages and each of these messages?
    // Are they independent of everything else?
    // Sends are independent of polls. Polls can return stale data, Can commitOffsets somehow get ahead of sends?
    // Can list offsets return stale data??? I don't think so! I don't think commit offsets can be buffered
    // If they say we're commited up to 2000 but that doesn't get written immediately, and they ask for
    // commited offsets,  we return a value lower than 2000, then they would start to reprocess those values
    // So we can't buffer committedOffsets in isolation. Maybe paired with cached reads?
    // How could they say they're commited up to a value if I don't write up to that value? Maybe there's a way to coordinate with send buffer to make it work? Must be able to. If the sends don't get written, they can't get polled, so the client can't process them and commit past what's been written.
    // Just write everything at once? Lock all reads while writing? Does that matter?


    // 2 concurrency issues to deal with.
    //   Requests while writes are being buffered.
    //     Here I think a naive approach should work. Stale poll are fine.
    //   Requests while the buffer is being written
    //     Have the double buffer to catch incoming writes. Or rather, just duplicate the buffer clear it and release it?
    //     What about reads though? Can you read while writing. I guess why not? Only thing I can think of would be the order of the writes fucking with the reads, but if you just write them in order it shouldn't be any different from getting a bunch of writes at once...


    // This in effect can reorder sends and offsets within themselves. If we do all sends before offsets. Eh, just do them in the order that you go them
    // What if we get a send or commit offset while writing? Hold the connetion open with locks? No, just put them
    // in another buffer. Lock the first buffer and start writing to the second buffer

    // Open issues:
    //      How does aggregating the writes affect things? single write for each log and offset
    //      Can you aggregate commitOffsets? Can you buffer it?
    writeBuffer := map[string][]interface{}{}
    writeBufferMutex := &sync.Mutex{}
    latestOffsets := [500]int{}
    latestOffsetsMutex := &sync.Mutex{}

    // Initialize the logs
    n.Handle("init", func(msg maelstrom.Message) error {
        if n.ID() == writerNodeID {
            for i := 0; i <= 500; i++ {
                writeLogCtx, writeLogCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                writeOffsetCtx, writeOffsetCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                defer writeLogCancel()
                defer writeOffsetCancel()
                kLogs.Write(writeLogCtx, "log-" + strconv.Itoa(i), []int{})
                kLogs.Write(writeOffsetCtx, "offset-" + strconv.Itoa(i), 0)
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
                logMessage := int(body["msg"].(float64))

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

                for logKey, proposedOffset := range committedOffsets {
                    offset := int(proposedOffset.(float64))
                    kLogCommittedOffsetsMutex.Lock()
                    readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                    defer readCancel()
                    kLogCommittedOffsetTemp, readErr := kLogs.Read(readCtx, "offset-" + logKey)
                    if readErr != nil {
                        return nil, readErr
                    }
                    kLogCommittedOffset := kLogCommittedOffsetTemp.(int)

                    if offset >= kLogCommittedOffset {
                        writeCtx, writeCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                        defer writeCancel()
                        kLogs.Write(writeCtx, "offset-"+logKey, offset)
                    }
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

        offsetResponses := map[string][][]int{}
        // Can this be done in parallel?
        for logKey, requestedOffset := range requestedOffsets {
            offset := int(requestedOffset.(float64))
            offsetMessages := [][]int{}
            readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
            defer readCancel()
            kLogTemp, readErr := kLogs.Read(readCtx, "log-" + logKey)
            if readErr != nil {
                return readErr
            }
            kLog := kLogTemp.([]interface{})

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

        // Does this need to be refactored to latestOffsets? Basically caching reads? sort of
        // If so, then do we need to store the committed offsets in the klog at all?
        // But then, do we need to store anything there? Well the offsets are *way* less memory
        // No, these are different sets of offsets. In memory is the offset of the last messsage
        // committed offsets it eh offset they've processed up to. That said, we could probably cache this in memory
        // How do you deal with server crashes though
        responseOffsets := map[string]int{}
        for _, requestedOffsetKey := range requestedOffsetKeys {
            offsetKey := requestedOffsetKey.(string)

            readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
            defer readCancel()
            offset, readErr := kLogs.Read(readCtx, "offset-" + offsetKey)
            if readErr != nil {
                return readErr
            }
            responseOffsets[offsetKey] = offset.(int)
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
                for logKey, bufferedMessages := range tempWriteBuffer {
                    kLogsMutex.Lock() // Don't actually thing this is necessary anymore. Only needed if previous tick is still running. We'll keep it.

                    // Read the previous value of the log
                    readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
                    defer readCancel()
                    kLog, _ := kLogs.Read(readCtx, "log-"+logKey)

                    // Append the new value
                    newKLog := append(kLog.([]interface{}), bufferedMessages...)

                    // Write the new log back to the kv
                    writeCtx, writeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
                    defer writeCancel()
                    kLogs.Write(writeCtx, "log-"+logKey, newKLog)

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
