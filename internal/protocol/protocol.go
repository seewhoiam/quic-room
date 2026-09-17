package protocol

import "encoding/json"

const (
        TypeJoin    = "join"
        TypeChat    = "chat"
        TypeAck     = "ack"
        TypeResume  = "resume"
        TypePing    = "ping"
        TypeWelcome = "welcome"
        TypeEvent   = "event"
        TypeSnapshot = "snapshot"
        TypePong    = "pong"
        TypeError   = "error"
        TypeBye     = "bye"

        EvMemberJoin  = "member_join"
        EvMemberLeave = "member_leave"
        EvChat        = "chat"
)

// Envelope is the outer JSON for every frame.
type Envelope struct {
        Type    string          `json:"type"`
        Payload json.RawMessage `json:"payload,omitempty"`
}

type Join struct {
        Room string `json:"room"`
        Name string `json:"name"`
}

type Chat struct {
        Text string `json:"text"`
}

type Ack struct {
        Seq uint64 `json:"seq"`
}

type Resume struct {
        Room         string `json:"room"`
        FromSeq      uint64 `json:"fromSeq"`
        SessionToken string `json:"sessionToken"`
}

type Welcome struct {
        SessionToken string   `json:"sessionToken"`
        SnapshotSeq  uint64   `json:"snapshotSeq"`
        Snapshot     Snapshot `json:"snapshot"`
}

type Event struct {
        Seq     uint64          `json:"seq"`
        Type    string          `json:"type"`
        Payload json.RawMessage `json:"payload"`
}

type Snapshot struct {
        Seq     uint64   `json:"seq"`
        Members []string `json:"members"`
        Chats   []string `json:"chats"`
}

type ErrorMsg struct {
        Code    string `json:"code"`
        Message string `json:"message"`
}

type Bye struct {
        Reason string `json:"reason"`
}

type MemberPayload struct {
        Name string `json:"name"`
}

type ChatPayload struct {
        Name string `json:"name"`
        Text string `json:"text"`
}
