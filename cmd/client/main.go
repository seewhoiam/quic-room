package main

import (
        "bufio"
        "context"
        "crypto/tls"
        "encoding/json"
        "flag"
        "fmt"
        "log"
        "os"
        "path/filepath"
        "strings"
        "time"

        "github.com/quic-go/quic-go"
        "github.com/seewhoiam/quic-room/internal/protocol"
)

type sessionFile struct {
        Token   string `json:"token"`
        LastSeq uint64 `json:"lastSeq"`
        Room    string `json:"room"`
        Name    string `json:"name"`
}

func main() {
        addr := flag.String("addr", "127.0.0.1:4242", "server address")
        roomName := flag.String("room", "demo", "room name")
        name := flag.String("name", "anon", "display name")
        insecure := flag.Bool("insecure", true, "skip TLS verify (local demo)")
        flag.Parse()

        tlsConf := &tls.Config{
                InsecureSkipVerify: *insecure,
                NextProtos:         []string{"quic-room"},
        }
        ctx := context.Background()
        conn, err := quic.DialAddr(ctx, *addr, tlsConf, nil)
        if err != nil {
                log.Fatal(err)
        }
        defer conn.CloseWithError(0, "bye")

        stream, err := conn.OpenStreamSync(ctx)
        if err != nil {
                log.Fatal(err)
        }

        sessPath := filepath.Join(os.TempDir(), fmt.Sprintf("quic-room-%s-%s.session", *roomName, *name))
        var lastSeq uint64
        var token string
        if b, err := os.ReadFile(sessPath); err == nil {
                var s sessionFile
                if json.Unmarshal(b, &s) == nil && s.Room == *roomName && s.Name == *name && s.Token != "" {
                        token, lastSeq = s.Token, s.LastSeq
                }
        }

        if token != "" {
                if err := protocol.WriteFrame(stream, protocol.TypeResume, protocol.Resume{
                        Room: *roomName, FromSeq: lastSeq, SessionToken: token,
                }); err != nil {
                        log.Fatal(err)
                }
                log.Printf("resume room=%s fromSeq=%d", *roomName, lastSeq)
        } else {
                if err := protocol.WriteFrame(stream, protocol.TypeJoin, protocol.Join{Room: *roomName, Name: *name}); err != nil {
                        log.Fatal(err)
                }
        }

        // reader
        go func() {
                for {
                        env, err := protocol.ReadFrame(stream)
                        if err != nil {
                                log.Printf("disconnected: %v", err)
                                os.Exit(0)
                        }
                        switch env.Type {
                        case protocol.TypeWelcome:
                                var w protocol.Welcome
                                _ = protocol.Decode(env.Payload, &w)
                                token = w.SessionToken
                                lastSeq = w.SnapshotSeq
                                saveSession(sessPath, *roomName, *name, token, lastSeq)
                                fmt.Printf("[welcome] members=%v chats=%v seq=%d\n", w.Snapshot.Members, w.Snapshot.Chats, w.SnapshotSeq)
                        case protocol.TypeSnapshot:
                                var s protocol.Snapshot
                                _ = protocol.Decode(env.Payload, &s)
                                lastSeq = s.Seq
                                saveSession(sessPath, *roomName, *name, token, lastSeq)
                                fmt.Printf("[snapshot] members=%v chats=%v seq=%d\n", s.Members, s.Chats, s.Seq)
                        case protocol.TypeEvent:
                                var e protocol.Event
                                _ = protocol.Decode(env.Payload, &e)
                                lastSeq = e.Seq
                                saveSession(sessPath, *roomName, *name, token, lastSeq)
                                _ = protocol.WriteFrame(stream, protocol.TypeAck, protocol.Ack{Seq: e.Seq})
                                fmt.Printf("[event %d %s] %s\n", e.Seq, e.Type, string(e.Payload))
                        case protocol.TypeError:
                                fmt.Printf("[error] %s\n", string(env.Payload))
                        case protocol.TypePong:
                                // ignore
                        case protocol.TypeBye:
                                fmt.Printf("[bye] %s\n", string(env.Payload))
                        default:
                                fmt.Printf("[%s] %s\n", env.Type, string(env.Payload))
                        }
                }
        }()

        // keepalive
        go func() {
                t := time.NewTicker(15 * time.Second)
                defer t.Stop()
                for range t.C {
                        _ = protocol.WriteFrame(stream, protocol.TypePing, nil)
                }
        }()

        sc := bufio.NewScanner(os.Stdin)
        fmt.Println("type chat and enter; Ctrl+C to quit (session saved for resume)")
        for sc.Scan() {
                text := strings.TrimSpace(sc.Text())
                if text == "" {
                        continue
                }
                if err := protocol.WriteFrame(stream, protocol.TypeChat, protocol.Chat{Text: text}); err != nil {
                        log.Fatal(err)
                }
        }
}

func saveSession(path, roomName, name, token string, seq uint64) {
        b, _ := json.Marshal(sessionFile{Token: token, LastSeq: seq, Room: roomName, Name: name})
        _ = os.WriteFile(path, b, 0o600)
}
