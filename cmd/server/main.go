package main

import (
        "context"
        "crypto/tls"
        "encoding/json"
        "flag"
        "io"
        "log"
        "sync"

        "github.com/quic-go/quic-go"
        "github.com/seewhoiam/quic-room/internal/protocol"
        "github.com/seewhoiam/quic-room/internal/room"
        "github.com/seewhoiam/quic-room/internal/tlsutil"
)

type clientConn struct {
        mu     sync.Mutex
        stream quic.Stream
        token  string
        room   *room.Room
        name   string
        lastAck uint64
}

func (c *clientConn) send(typ string, payload any) error {
        c.mu.Lock()
        defer c.mu.Unlock()
        return protocol.WriteFrame(c.stream, typ, payload)
}

func main() {
        addr := flag.String("addr", "127.0.0.1:4242", "listen address")
        flag.Parse()

        cert, err := tlsutil.GenerateSelfSigned()
        if err != nil {
                log.Fatal(err)
        }
        tlsConf := &tls.Config{
                Certificates: []tls.Certificate{cert},
                NextProtos:   []string{"quic-room"},
        }
        ln, err := quic.ListenAddr(*addr, tlsConf, &quic.Config{EnableDatagrams: false})
        if err != nil {
                log.Fatal(err)
        }
        log.Printf("quic-room server listening on %s", *addr)
        hub := room.NewHub(1024)

        for {
                conn, err := ln.Accept(context.Background())
                if err != nil {
                        log.Printf("accept: %v", err)
                        continue
                }
                go handleConn(conn, hub)
        }
}

func handleConn(conn quic.Connection, hub *room.Hub) {
        defer conn.CloseWithError(0, "bye")
        stream, err := conn.AcceptStream(context.Background())
        if err != nil {
                return
        }
        defer stream.Close()

        cc := &clientConn{stream: stream}
        defer func() {
                if cc.room != nil && cc.token != "" {
                        if ev, ok := cc.room.Leave(cc.token); ok {
                                broadcast(cc.room, nil, protocol.TypeEvent, protocol.Event{
                                        Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload,
                                })
                        }
                }
        }()

        for {
                env, err := protocol.ReadFrame(stream)
                if err != nil {
                        if err != io.EOF {
                                log.Printf("read: %v", err)
                        }
                        return
                }
                if err := dispatch(cc, hub, env); err != nil {
                        _ = cc.send(protocol.TypeError, protocol.ErrorMsg{Code: "bad_request", Message: err.Error()})
                }
        }
}

var (
        roomClientsMu sync.Mutex
        roomClients   = map[*room.Room]map[*clientConn]struct{}{}
)

func track(r *room.Room, c *clientConn, add bool) {
        roomClientsMu.Lock()
        defer roomClientsMu.Unlock()
        m := roomClients[r]
        if m == nil {
                m = map[*clientConn]struct{}{}
                roomClients[r] = m
        }
        if add {
                m[c] = struct{}{}
        } else {
                delete(m, c)
                if len(m) == 0 {
                        delete(roomClients, r)
                }
        }
}

func broadcast(r *room.Room, except *clientConn, typ string, payload any) {
        roomClientsMu.Lock()
        clients := make([]*clientConn, 0, len(roomClients[r]))
        for c := range roomClients[r] {
                if c != except {
                        clients = append(clients, c)
                }
        }
        roomClientsMu.Unlock()
        for _, c := range clients {
                _ = c.send(typ, payload)
        }
}

func dispatch(cc *clientConn, hub *room.Hub, env protocol.Envelope) error {
        switch env.Type {
        case protocol.TypePing:
                return cc.send(protocol.TypePong, nil)
        case protocol.TypeJoin:
                var p protocol.Join
                if err := protocol.Decode(env.Payload, &p); err != nil {
                        return err
                }
                r := hub.Get(p.Room)
                token, snap, ev, err := r.Join(p.Name)
                if err != nil {
                        return err
                }
                cc.room, cc.token, cc.name = r, token, p.Name
                track(r, cc, true)
                if err := cc.send(protocol.TypeWelcome, protocol.Welcome{
                        SessionToken: token, SnapshotSeq: snap.Seq, Snapshot: snap,
                }); err != nil {
                        return err
                }
                broadcast(r, cc, protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload})
                return nil
        case protocol.TypeChat:
                if cc.room == nil {
                        return errStr("not joined")
                }
                var p protocol.Chat
                if err := protocol.Decode(env.Payload, &p); err != nil {
                        return err
                }
                ev, err := cc.room.Chat(cc.token, p.Text)
                if err != nil {
                        return err
                }
                broadcast(cc.room, nil, protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload})
                return nil
        case protocol.TypeAck:
                var p protocol.Ack
                if err := protocol.Decode(env.Payload, &p); err != nil {
                        return err
                }
                cc.lastAck = p.Seq
                return nil
        case protocol.TypeResume:
                var p protocol.Resume
                if err := protocol.Decode(env.Payload, &p); err != nil {
                        return err
                }
                r := hub.Get(p.Room)
                // session token encodes name prefix for demo; require token present in room or re-bind by name in token
                name := p.SessionToken
                if i := indexByte(name, '-'); i >= 0 {
                        name = name[:i]
                }
                r.EnsureSession(p.SessionToken, name)
                cc.room, cc.token, cc.name = r, p.SessionToken, name
                track(r, cc, true)
                needSnap, snap, evs := r.Resume(p.FromSeq)
                if needSnap {
                        if err := cc.send(protocol.TypeSnapshot, snap); err != nil {
                                return err
                        }
                } else {
                        // still send welcome-like ack of resume via snapshot of empty? send events only
                        for _, ev := range evs {
                                if err := cc.send(protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload}); err != nil {
                                        return err
                                }
                        }
                }
                if needSnap {
                        // after snapshot, no need to replay; client is caught up to snap.Seq
                        _ = snap
                }
                return nil
        default:
                return errStr("unknown type " + env.Type)
        }
}

type strErr string

func (e strErr) Error() string { return string(e) }
func errStr(s string) error    { return strErr(s) }

func indexByte(s string, c byte) int {
        for i := 0; i < len(s); i++ {
                if s[i] == c {
                        return i
                }
        }
        return -1
}

// silence unused import if any
var _ = json.Marshal
