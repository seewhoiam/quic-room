package room

import (
        "encoding/json"
        "fmt"
        "sync"

        "github.com/seewhoiam/quic-room/internal/protocol"
)

const (
        DefaultLogCap   = 1024
        DefaultChatKeep = 50
)

type Event struct {
        Seq     uint64
        Type    string
        Payload json.RawMessage
}

type Hub struct {
        mu    sync.Mutex
        rooms map[string]*Room
        logCap int
}

func NewHub(logCap int) *Hub {
        if logCap <= 0 {
                logCap = DefaultLogCap
        }
        return &Hub{rooms: make(map[string]*Room), logCap: logCap}
}

func (h *Hub) Get(name string) *Room {
        h.mu.Lock()
        defer h.mu.Unlock()
        r, ok := h.rooms[name]
        if !ok {
                r = newRoom(name, h.logCap)
                h.rooms[name] = r
        }
        return r
}

type Session struct {
        Token string
        Name  string
}

type Room struct {
        name   string
        mu     sync.Mutex
        seq    uint64
        log    []Event
        logCap int
        chats  []string
        members map[string]string // token -> name
}

func newRoom(name string, logCap int) *Room {
        return &Room{
                name:    name,
                logCap:  logCap,
                members: make(map[string]string),
        }
}

func (r *Room) Snapshot() protocol.Snapshot {
        r.mu.Lock()
        defer r.mu.Unlock()
        members := make([]string, 0, len(r.members))
        for _, n := range r.members {
                members = append(members, n)
        }
        chats := append([]string(nil), r.chats...)
        return protocol.Snapshot{Seq: r.seq, Members: members, Chats: chats}
}

func (r *Room) Join(name string) (token string, snap protocol.Snapshot, ev Event, err error) {
        r.mu.Lock()
        defer r.mu.Unlock()
        token = fmt.Sprintf("%s-%d", name, r.seq+1)
        r.members[token] = name
        payload, _ := json.Marshal(protocol.MemberPayload{Name: name})
        ev = r.appendLocked(protocol.EvMemberJoin, payload)
        snap = protocol.Snapshot{
                Seq:     r.seq,
                Members: memberNames(r.members),
                Chats:   append([]string(nil), r.chats...),
        }
        return token, snap, ev, nil
}

func (r *Room) Leave(token string) (ev Event, ok bool) {
        r.mu.Lock()
        defer r.mu.Unlock()
        name, exists := r.members[token]
        if !exists {
                return Event{}, false
        }
        delete(r.members, token)
        payload, _ := json.Marshal(protocol.MemberPayload{Name: name})
        return r.appendLocked(protocol.EvMemberLeave, payload), true
}

func (r *Room) Chat(token, text string) (ev Event, err error) {
        r.mu.Lock()
        defer r.mu.Unlock()
        name, ok := r.members[token]
        if !ok {
                return Event{}, fmt.Errorf("unknown session")
        }
        line := name + ": " + text
        r.chats = append(r.chats, line)
        if len(r.chats) > DefaultChatKeep {
                r.chats = r.chats[len(r.chats)-DefaultChatKeep:]
        }
        payload, _ := json.Marshal(protocol.ChatPayload{Name: name, Text: text})
        return r.appendLocked(protocol.EvChat, payload), nil
}

func (r *Room) ValidateSession(token string) bool {
        r.mu.Lock()
        defer r.mu.Unlock()
        _, ok := r.members[token]
        return ok
}

func (r *Room) EnsureSession(token, name string) {
        r.mu.Lock()
        defer r.mu.Unlock()
        if _, ok := r.members[token]; !ok {
                r.members[token] = name
        }
}

// Resume returns events with seq > fromSeq. If fromSeq is too old, needSnapshot is true.
func (r *Room) Resume(fromSeq uint64) (needSnapshot bool, snap protocol.Snapshot, events []Event) {
        r.mu.Lock()
        defer r.mu.Unlock()
        if len(r.log) == 0 {
                if fromSeq < r.seq {
                        // empty log but seq advanced somehow — treat as snapshot
                        return true, protocol.Snapshot{Seq: r.seq, Members: memberNames(r.members), Chats: append([]string(nil), r.chats...)}, nil
                }
                return false, protocol.Snapshot{}, nil
        }
        oldest := r.log[0].Seq
        if fromSeq+1 < oldest {
                return true, protocol.Snapshot{Seq: r.seq, Members: memberNames(r.members), Chats: append([]string(nil), r.chats...)}, nil
        }
        out := make([]Event, 0)
        for _, e := range r.log {
                if e.Seq > fromSeq {
                        out = append(out, e)
                }
        }
        return false, protocol.Snapshot{}, out
}

func (r *Room) appendLocked(typ string, payload json.RawMessage) Event {
        r.seq++
        ev := Event{Seq: r.seq, Type: typ, Payload: payload}
        r.log = append(r.log, ev)
        if len(r.log) > r.logCap {
                r.log = r.log[len(r.log)-r.logCap:]
        }
        return ev
}

func memberNames(m map[string]string) []string {
        out := make([]string, 0, len(m))
        for _, n := range m {
                out = append(out, n)
        }
        return out
}
