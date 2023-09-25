package main

import (
    "log"
    "os"
    // "github.com/google/uuid"
    // "encoding/json"
    // "reflect"
    // "fmt"
    // "strconv"
    // "strings"
    // "sync"
    // "math/rand"
    // "time"
    // "context"
    maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)


func main() {
    n := maelstrom.NewNode()


    if err := n.Run(); err != nil {
        log.Printf("ERROR: %s", err)
        os.Exit(1)
    }
}
