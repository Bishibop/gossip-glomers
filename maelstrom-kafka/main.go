package main


// Whats going on the with unending send ok replys
// Seems like we're never falling into the idempotency check?
// We don't seem to be removing keys from the pendingForwards set correctly. We forwarded a message, got the send_ok back and the continued to forward it later. But only when we had an issue with another forward?
// After another no key error, I got the same double forwards.

// TODO:
// * Buffer writes?
// * Cache reads?
// * Could still do some divided writing. So go back to dividing the logs for writing Wit ht emod. Just hard code it for 2 noes. Probably not a big deal for performance with 2 nodes
// * Biggest gains will probably be in caching read info rather then requesting whole logs.
// * Implement zipper idea. 2 datasets in the kv. Published logs for reading. Smaller write space for segmented writes paired with a periodic processing of the write space into the published logs. Works because reads don't have to be up to date





// Questions:
//  what does it mean when they say that not every offset must contain a message?
//  why would you skip offsets?
//    I can see if you divided them into sets of arrays for some efficient lookup. so like 1-1000, 1001-2000, etc
//    and then you would have large gaps
//  Do offsets need to be unique across logs?
// Notes:
//    There can be gaps in offsets
//    No recency requirement. Poll does not have to return the most up to date messages coming in through send
//    Poll does have to return messages without skipping any though
//    Offset log should always increase. No inserting message before previous messages. Append only
// Approach:
//    How to store the logs? Hash of arrays is the simplest that I can think of. Drawbacks...
//
//    How am I handling the inialization of the different logs? could do it dynamically, or just watch the logs for failed messages and hardcode whatever the problem is asking for. We'll do this. Then the code can just rely on them being there


// Potential Optimizations:
//   * Don't send the whole log on reads. Somehow segment the log into digestible units
//   * Keep some info locally in the node? What would I keep?
//   * Segment which logs each node cares about? I mean, if the logs are stored in the kv, then what advantage to that is there here
//     * If you do this, you forward the other node's writes to them. But what happens if they get a write in the meantime? Do writes need to be in order across the whole system?
//     * Do need to forward reads? No. Any node can read.
//     * How does this handle if a node goes down? Get's into the idea of "promotion". Possibly out of the scope of the challenge? I think all it does is partition the nodes. But that's enough to break the segmentation idea. If you're partitioned, how do you handle that. I guess you could buffer those as well... Then your writes get *really* out of order. Again, maybe that doesn't matter. Problem only talked aout read/write consistency
//     * Just keep a list of write forwards, keep retrying them until you get an ok, then remove them. Need to make sure it's idempotent, eugh. WHY CAN"T I JUST HAVE EVERY NODE WRITE
//     * WAIT. What if I have every node write to it's own subset/key in the log... No conflicts, no forwarding. But what about guaranteeing read/write consistency. Yeah, how do you handle the offsets... Some single offset oracle? I wonder if there's a way to convert timestamps to monotonically increasing integers. I mean, you can just use the nonosecond timestamp.
//   * Buffer writes. Maybe not relevant to the problem, but this increases the risk of losing any data in the buffer.
//     * If you buffer writes, do you have to hold reads?I think the problem said no? As long as they're in order
//   * Data structure - just write the log offsets in to the keys? Lol, you'd have to do a read for every message
//   * Cache reads
//   * Break up the logs into smaller segments so you don't have to send the whole log in every read? This seems like caching reads. Maybe have some sliding window "delta" seperate from the whole log that you can request and stitch into a cached version of the whole log
//   * You could have one mutex for every log rather than every node
//   * Ohhh, the writer node system, maxes out your scalability to the number of logs you have. That's a huge drawwback




import (
    "log"
    "os"
    "github.com/google/uuid"
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

func main() {
    n := maelstrom.NewNode()
    writerNodeID := "n1"
    kLogs := maelstrom.NewLinKV(n)
    kLogsMutex := &sync.Mutex{}
    kLogCommittedOffsetsMutex := &sync.Mutex{}
    type PendingForward struct {
        destinationID string
        msgBody interface{}
    }
    pendingForwards := map[string]PendingForward{} // A set as well
    processedForwards := map[string]bool{}
    // some kind of pending forwarded array
    // put every forwarded message in here, oh but how do we know it's been processed? Need another handler for forwarded message?
    // who replies to the client? both?
    // do you reply to the client immediately? or after the job is done? let's just do it when the job is done
    // we'll have the final handler node do both. Reply to the original message and send a confirmation to the forwarding node
    // what about concurrency here as well? :) if you just append and copy, then you could copy over while another message is being removed. just use a mutex

    // Initialize the logs
    // I wonder if these lin-kv requests won't finish before the inbound requests start coming in?
    n.Handle("init", func(msg maelstrom.Message) error {
        if n.ID() == "n1" {
            for i := 0; i <= 500; i++ {
                writeLogCtx, writeLogCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                writeOffsetCtx, writeOffsetCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                defer writeLogCancel()
                defer writeOffsetCancel()
                kLogs.Write(writeLogCtx, "log-" + strconv.Itoa(i), []int{})
                kLogs.Write(writeOffsetCtx, "offset-" + strconv.Itoa(i), 0)
            }
        }

        // response := map[string]interface{}{ "type": "init_ok" }
        // return n.Reply(msg, response)
        return nil
    })


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
    //
    //
    //          hmm, is this seems dumb, reads should be much more common that writes?, well no, it's a log. opposite should be true.
    //          Supports buffering writes
    //
    // Read the appropriate log. Append to it, write it back
    // Issue. how do you deal with multipl enodes writing to the same log.
    //      Mutex lock then CAS. Actually this won't work. Mutex's don't lock across notes of course.
    //      CAS with retries. Eugh.
    //      Segment logs across nodes. Then you ust need to lock across multiple requests to the same node. Is that any different? Yes, here mutexes would work
    // Does this have anything to do with linearizability? Linearizability only helps with multile writes fromt he same node in a row? what does liearizability get me in this problem
    // This is where you could segment the log. If only one node handles one log, then it can just use a mutx on it's won requests. Or a channel.Or buffer the writes
    //
    //
    //
    // Alright, I'm just going to do a dedicated writer node

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
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        logKey := body["key"].(string)
        logMessage := int(body["msg"].(float64))

        if n.ID() == writerNodeID {
            // write to the log

            // Idempotency check
            if body["forwardUniqueID"] != nil && processedForwards[body["forwardUniqueID"].(string)] {
                // Already processed this message
                // Reply to sender so they can remove the message from their pending forwards
                return n.Reply(msg, map[string]interface{}{
                    "type": "forward_confirm",
                    "forwardUniqueID": body["forwardUniqueID"],
                })
            } else {
                // Locking across a network request. Feels wrong. Would channels do any better?
                // No... CAS likely only superior solution
                kLogsMutex.Lock()

                // Read the previous value of the log
                readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
                defer readCancel()
                kLog, readErr := kLogs.Read(readCtx, "log-"+logKey)
                if readErr != nil {
                    return readErr
                }

                // Append the new value
                newKLog := append(kLog.([]interface{}), logMessage)

                // Write the new log back to the kv
                writeCtx, writeCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
                defer writeCancel()
                kLogs.Write(writeCtx, "log-"+logKey, newKLog)

                kLogsMutex.Unlock()

                response := map[string]interface{}{
                    "type": "send_ok",
                    "offset": len(newKLog) - 1,
                }

                // If this was a forwarded messages
                if body["forwardUniqueID"] != nil {
                    // Respond to the original (external) sender
                    n.Send(body["originalSenderID"].(string), response)
                    processedForwards[body["forwardUniqueID"].(string)] = true
                    // Respond to the fowarding node
                    return n.Reply(msg, map[string]interface{}{
                        "type": "forward_confirm",
                        "forwardUniqueID": body["forwardUniqueID"],
                    })
                } else {
                    return n.Reply(msg, response)
                }
            }
        } else {
            // Forward to the appropriate writer node
            forwardUniquenessID := uuid.New().String()
            // body["type"] = "send" // maybe I need to set this?
            body["originalSenderID"] = msg.Src
            body["forwardUniqueID"] = forwardUniquenessID // for idempotency
            pendingForwards[forwardUniquenessID] = PendingForward{destinationID: writerNodeID, msgBody: body}
            return n.Send(writerNodeID, body)
            // return n.RPC(writerNodeID, body, func(msg maelstrom.Message) error {
            //     // remove from the retry set
            //     var body map[string]interface{}
            //     if err := json.Unmarshal(msg.Body, &body); err != nil {
            //         return err
            //     }
            //     delete(pendingForwards, body["forwardUniqueID"].(string))
            //     return nil
            // })
        }
    })

    n.Handle("forward_confirm", func(msg maelstrom.Message) error {
        // remove from the retry set
        log.Fatal("failed in forward_confirm")
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        delete(pendingForwards, body["forwardUniqueID"].(string))
        return nil
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
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }
        committedOffsets := body["offsets"].(map[string]interface{})

        if n.ID() == writerNodeID {
            // for each, read it, compare it, then write it
            if body["forwardUniqueID"] != nil && processedForwards[body["forwardUniqueID"].(string)] {
                // Already processed this message
                // Reply to sender so they can remove the message from their pending forwards
                return n.Reply(msg, map[string]interface{}{
                    "type": "forward_confirm",
                    "forwardUniqueID": body["forwardUniqueID"],
                })
            } else {
                for logKey, proposedOffset := range committedOffsets {
                    offset := int(proposedOffset.(float64))
                    kLogCommittedOffsetsMutex.Lock()
                    readCtx, readCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                    defer readCancel()
                    kLogCommittedOffsetTemp, readErr := kLogs.Read(readCtx, "offset-" + logKey)
                    if readErr != nil {
                        return readErr
                    }
                    kLogCommittedOffset := kLogCommittedOffsetTemp.(int)

                    if offset >= kLogCommittedOffset {
                        writeCtx, writeCancel := context.WithTimeout(context.Background(), 100 * time.Millisecond)
                        defer writeCancel()
                        kLogs.Write(writeCtx, "offset-"+logKey, offset)
                    }
                    kLogCommittedOffsetsMutex.Unlock()
                }

                response := map[string]interface{}{
                    "type": "commit_offsets_ok",
                }
                if body["forwardUniqueID"] != nil {
                    // Respond to the original (external) sender
                    n.Send(body["originalSenderID"].(string), response)
                    processedForwards[body["forwardUniqueID"].(string)] = true
                    // Respond to the fowarding node
                    return n.Reply(msg, map[string]interface{}{
                        "type": "forward_confirm",
                        "forwardUniqueID": body["forwardUniqueID"],
                    })
                } else {
                    return n.Reply(msg, response)
                }
            }
        } else {
            // Forward to the appropriate writer node
            forwardUniquenessID := uuid.New().String()
            // body["type"] = "send" // maybe I need to set this?
            body["originalSenderID"] = msg.Src
            body["forwardUniqueID"] = forwardUniquenessID // for idempotency
            pendingForwards[forwardUniquenessID] = PendingForward{destinationID: writerNodeID, msgBody: body}
            // return n.Send(writerNodeID, body)
            return n.RPC(writerNodeID, body, func(msg maelstrom.Message) error {
                // remove from the retry set
                var body map[string]interface{}
                if err := json.Unmarshal(msg.Body, &body); err != nil {
                    return err
                }
                delete(pendingForwards, body["forwardUniqueID"].(string))
                return nil
            })
        }
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
        for logKey, requestedOffset := range requestedOffsets {
            offset := int(requestedOffset.(float64))
            offsetMessages := [][]int{}
            readCtx, readCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
            defer readCancel()
            kLogTemp, readErr := kLogs.Read(readCtx, "log-"+logKey)
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
        forwardRetryTicker := time.NewTicker(time.Second / 25)
        defer forwardRetryTicker.Stop()
        for range forwardRetryTicker.C {
            for _, pendingForward := range pendingForwards {
                n.Send(pendingForward.destinationID, pendingForward.msgBody)
            }
        }
    }()

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
