// Package protocol 定义 quic-room 的线协议：
// 每个帧是一个 4 字节大端长度前缀 + JSON 编码的 Envelope。
package protocol

import "encoding/json"

// 帧类型常量：客户端请求（join/chat/ack/resume/ping）与服务端推送
// （welcome/event/snapshot/pong/error/bye）。
const (
	TypeJoin     = "join"     // 客户端 -> 服务端：加入房间
	TypeChat     = "chat"     // 客户端 -> 服务端：发送聊天消息
	TypeAck      = "ack"      // 客户端 -> 服务端：确认已收到的事件序号
	TypeResume   = "resume"   // 客户端 -> 服务端：断线续传
	TypePing     = "ping"     // 客户端 -> 服务端：心跳
	TypeWelcome  = "welcome"  // 服务端 -> 客户端：join 成功，附会话令牌与快照
	TypeEvent    = "event"    // 服务端 -> 客户端：房间事件（成员变动/聊天）
	TypeSnapshot = "snapshot" // 服务端 -> 客户端：全量快照（续传缺口太大时使用）
	TypePong     = "pong"     // 服务端 -> 客户端：心跳应答
	TypeError    = "error"    // 服务端 -> 客户端：请求出错
	TypeBye      = "bye"      // 服务端 -> 客户端：主动断开

	// 事件类型（Event.Type 的取值）
	EvMemberJoin  = "member_join"  // 有成员加入
	EvMemberLeave = "member_leave" // 有成员离开
	EvChat        = "chat"         // 聊天消息
)

// Envelope 是每个帧的外层 JSON 包装；Payload 的具体类型由 Type 决定，
// 使用 json.RawMessage 延迟解码。
type Envelope struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// Join 是 TypeJoin 的载荷：请求加入指定房间。
type Join struct {
	Room string `json:"room"`
	Name string `json:"name"` // 显示名
}

// Chat 是 TypeChat 的载荷：发送一条聊天文本。
type Chat struct {
	Text string `json:"text"`
}

// Ack 是 TypeAck 的载荷：客户端确认已处理到的事件序号。
type Ack struct {
	Seq uint64 `json:"seq"`
}

// Resume 是 TypeResume 的载荷：凭会话令牌从断点续传。
type Resume struct {
	Room         string `json:"room"`
	FromSeq      uint64 `json:"fromSeq"`      // 客户端已收到的最大事件序号
	SessionToken string `json:"sessionToken"` // 上次 Welcome 下发的令牌
}

// Welcome 是 TypeWelcome 的载荷：join 成功后下发，
// 包含续传所需的会话令牌和当前房间全量快照。
type Welcome struct {
	SessionToken string   `json:"sessionToken"`
	SnapshotSeq  uint64   `json:"snapshotSeq"`
	Snapshot     Snapshot `json:"snapshot"`
}

// Event 是 TypeEvent 的载荷：一条带序号的房间事件。
type Event struct {
	Seq     uint64          `json:"seq"`
	Type    string          `json:"type"` // EvMemberJoin / EvMemberLeave / EvChat
	Payload json.RawMessage `json:"payload"`
}

// Snapshot 是房间某一时刻的全量状态。
type Snapshot struct {
	Seq     uint64   `json:"seq"`     // 快照对应的事件序号
	Members []string `json:"members"` // 当前成员显示名列表
	Chats   []string `json:"chats"`   // 近期聊天记录（含 "名字: 内容" 前缀）
}

// ErrorMsg 是 TypeError 的载荷。
type ErrorMsg struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Bye 是 TypeBye 的载荷。
type Bye struct {
	Reason string `json:"reason"`
}

// MemberPayload 是 EvMemberJoin / EvMemberLeave 事件的载荷。
type MemberPayload struct {
	Name string `json:"name"`
}

// ChatPayload 是 EvChat 事件的载荷。
type ChatPayload struct {
	Name string `json:"name"`
	Text string `json:"text"`
}
