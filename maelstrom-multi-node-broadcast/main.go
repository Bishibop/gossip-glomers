// TODO:
// * Refactor shared state with mutexes to use manager goroutines and channels
// * Refactor to use RPC rather than Send
package main

import (
    "log"
    "os"
    // "github.com/google/uuid"
    "encoding/json"
    // "reflect"
    "fmt"
    "sync"
    "time"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

func main() {
    n := maelstrom.NewNode()
    topology := map[string][]string{
        "n0": []string{"n1", "n2", "n3", "n4"},
        "n5": []string{"n6", "n7", "n8", "n9"},
        "n10": []string{"n11", "n12", "n13", "n14"},
        "n15": []string{"n16", "n17", "n18", "n19"},
        "n20": []string{"n21", "n22", "n23", "n24"},
    }
    type NodeState struct {
        state string
        unresponsiveCountdown *time.Timer
    }
    nodeStates := map[string]*NodeState{}
    for i := 0; i < 25; i++ {
        nodeID := fmt.Sprintf("n%d", i)
        initialTimer := time.NewTimer(time.Second / 1000)
        initialTimer.Stop()
        nodeStates[nodeID] = &NodeState{state: "responsive", unresponsiveCountdown: initialTimer}
    }
    nodeStatesMutex := &sync.Mutex{}
    receivedMessages := map[float64]struct{}{}
    messageBuffer := []float64{}
    messagesMutex := &sync.Mutex{}
    var leaderP bool

    n.Handle("broadcast", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        message := body["message"].(float64)
        messagesMutex.Lock()
        if _, ok := receivedMessages[message]; !ok {
            receivedMessages[message] = struct{}{}
            if leaderP {
                messageBuffer = append(messageBuffer, message)
            }
            messagesMutex.Unlock()

            rebroadcast_body := map[string]interface{}{
                "type": "rebroadcast",
                "message": message,
            }
            nodeStatesMutex.Lock()
            for leaderId, _ := range topology {
                nodeState := nodeStates[leaderId]
                if leaderId != n.ID() && nodeState.state == "responsive" {
                    nodeState.unresponsiveCountdown.Stop()
                    nodeState.unresponsiveCountdown = time.AfterFunc(time.Second / 4, func() {
                        nodeState.state = "unresponsive"
                    })
                    n.Send(leaderId, rebroadcast_body)
                }
            }
            nodeStatesMutex.Unlock()
        } else {
            messagesMutex.Unlock()
        }

        response := map[string]interface{}{
            "type": "broadcast_ok",
        }
        return n.Reply(msg, response)
    })

    n.Handle("rebroadcast", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        message := body["message"].(float64)
        messagesMutex.Lock()
        if _, ok := receivedMessages[message]; !ok {
            receivedMessages[message] = struct{}{}
            if leaderP {
                messageBuffer = append(messageBuffer, message)
            }
        }
        messagesMutex.Unlock()

        response := map[string]interface{}{
            "type": "rebroadcast_ok",
        }
        return n.Reply(msg, response)
    })

    n.Handle("rebroadcast_ok", func(msg maelstrom.Message) error {
        nodeStatesMutex.Lock()
        nodeStates[msg.Src].unresponsiveCountdown.Stop()
        nodeStatesMutex.Unlock()

        return nil
    })

    n.Handle("rebroadcast_set", func(msg maelstrom.Message) error {
        var body map[string]interface{}
        if err := json.Unmarshal(msg.Body, &body); err != nil {
            return err
        }

        messagesMutex.Lock()
        for _, message := range body["messages"].([]interface{}) {
            if _, ok := receivedMessages[message.(float64)]; !ok {
                receivedMessages[message.(float64)] = struct{}{}
                if leaderP {
                    messageBuffer = append(messageBuffer, message.(float64))
                }
            }
        }
        messagesMutex.Unlock()

        response := map[string]interface{}{
            "type": "rebroadcast_set_ok",
        }
        return n.Reply(msg, response)
    })

    n.Handle("rebroadcast_set_ok", func(msg maelstrom.Message) error {
        nodeStatesMutex.Lock()
        nodeStates[msg.Src].unresponsiveCountdown.Stop()
        nodeStatesMutex.Unlock()

        return nil
    })

    n.Handle("ping", func(msg maelstrom.Message) error {
        response := map[string]interface{}{
            "type": "ping_ok",
        }
        return n.Reply(msg, response)
    })

    n.Handle("ping_ok", func(msg maelstrom.Message) error {
        messagesMutex.Lock()
        messages := make([]float64, 0, len(receivedMessages))
        for key := range receivedMessages {
          messages = append(messages, key)
        }
        messagesMutex.Unlock()

        nodeStatesMutex.Lock()
        nodeState := nodeStates[msg.Src]
        nodeState.state = "responsive"
        nodeState.unresponsiveCountdown.Stop()
        nodeState.unresponsiveCountdown = time.AfterFunc(time.Second / 4, func() {
            nodeState.state = "unresponsive"
        })
        nodeStatesMutex.Unlock()

        if len(messages) > 0 {
            response := map[string]interface{}{
                "type": "rebroadcast_set",
                "messages": messages,
            }
            return n.Send(msg.Src, response)
        } else {
            return nil
        }
    })


    n.Handle("read", func(msg maelstrom.Message) error {
        messagesMutex.Lock()
        messages := make([]float64, 0, len(receivedMessages))
        for key := range receivedMessages {
          messages = append(messages, key)
        }
        messagesMutex.Unlock()

        response := map[string]interface{}{
            "type": "read_ok",
            "messages": messages,
        }
        return n.Reply(msg, response)
    })

    n.Handle("topology", func(msg maelstrom.Message) error {
        if _, ok := topology[n.ID()]; ok {
            leaderP = true
        }

        response := map[string]interface{}{
            "type": "topology_ok",
        }
        return n.Reply(msg, response)
    })

    go func() {
        pingTicker := time.NewTicker(time.Second / 4)
        defer pingTicker.Stop()
        for range pingTicker.C {
            nodeStatesMutex.Lock()
            for nodeId, nodeState := range nodeStates {
                if nodeState.state == "unresponsive" {
                    n.Send(nodeId, map[string]interface{}{
                        "type": "ping",
                    })
                }
            }
            nodeStatesMutex.Unlock()
        }
    }()

    go func() {
        broadcastSetTicker := time.NewTicker(time.Second / 10)
        defer broadcastSetTicker.Stop()
        for range broadcastSetTicker.C {
            if leaderP {
                messagesMutex.Lock()
                messages := messageBuffer
                messageBuffer = nil
                messagesMutex.Unlock()

                if len(messages) > 0 {
                    nodeStatesMutex.Lock()

                    for _, followerId := range topology[n.ID()] {
                        followerState := nodeStates[followerId]
                        if followerState.state == "responsive" {
                            followerState.unresponsiveCountdown.Stop()
                            followerState.unresponsiveCountdown = time.AfterFunc(time.Second / 4, func() {
                                followerState.state = "unresponsive"
                            })
                            n.Send(followerId, map[string]interface{}{
                                "type": "rebroadcast_set",
                                "messages": messages,
                            })
                        }
                    }
                    nodeStatesMutex.Unlock()
                }
            }
        }
    }()

    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
