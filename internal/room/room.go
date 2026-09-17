// Package room 实现聊天室的核心领域逻辑：房间管理、成员会话、
// 事件日志（用于断线续传）与快照。
package room

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/seewhoiam/quic-room/internal/protocol"
)

const (
	DefaultLogCap   = 1024 // 事件日志默认容量（超出后丢弃最旧事件）
	DefaultChatKeep = 50   // 快照中保留的最近聊天条数
)

// Event 是房间内发生的一条带序号的事件。
type Event struct {
	Seq     uint64          // 单调递增序号，断线续传以此对齐
	Type    string          // protocol.EvMemberJoin / EvMemberOnline / EvMemberOffline / EvChat
	Payload json.RawMessage // 事件载荷，延迟解码
}

// Hub 管理所有房间，按名字惰性创建。
type Hub struct {
	mu     sync.Mutex
	rooms  map[string]*Room
	logCap int // 新建房间的事件日志容量
}

// NewHub 创建 Hub；logCap <= 0 时使用 DefaultLogCap。
func NewHub(logCap int) *Hub {
	if logCap <= 0 {
		logCap = DefaultLogCap
	}
	return &Hub{rooms: make(map[string]*Room), logCap: logCap}
}

// Get 返回指定名字的房间，不存在则创建。
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

// Session 表示房间中的一个成员会话。
type Session struct {
	Token string // 会话令牌，join 时签发
	Name  string // 显示名
}

// Room 是单个聊天室：维护成员、事件日志与近期聊天。
// 所有字段都受 mu 保护。
type Room struct {
	name    string
	mu      sync.Mutex
	seq     uint64            // 当前最新事件序号
	log     []Event           // 环形截断的事件日志，供 resume 重放
	logCap  int               // 日志容量上限
	chats   []string          // 近期聊天记录（"名字: 内容"）
	members map[string]string // token -> 显示名
	online  map[string]bool   // token -> 是否在线（断连只标记离线，成员关系保留以支持续传）
}

func newRoom(name string, logCap int) *Room {
	return &Room{
		name:    name,
		logCap:  logCap,
		members: make(map[string]string),
		online:  make(map[string]bool),
	}
}

// Snapshot 返回房间当前状态的全量快照。
// 成员列表包含离线成员（断连只标记离线，会话保留）。
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

// Join 让一名成员加入房间：签发会话令牌，记录 join 事件，
// 并返回加入后的快照。token 形如 "名字-序号"（demo 用，可据此解析出名字）。
func (r *Room) Join(name string) (token string, snap protocol.Snapshot, ev Event, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	token = fmt.Sprintf("%s-%d", name, r.seq+1)
	r.members[token] = name
	r.online[token] = true
	payload, _ := json.Marshal(protocol.MemberPayload{Name: name})
	ev = r.appendLocked(protocol.EvMemberJoin, payload)
	snap = protocol.Snapshot{
		Seq:     r.seq,
		Members: memberNames(r.members),
		Chats:   append([]string(nil), r.chats...),
	}
	return token, snap, ev, nil
}

// MarkOffline 把指定令牌的成员标记为离线（连接断开时调用）。
// 成员关系保留在房间内，之后可用同一令牌 resume 回来；
// 仅当成员当前在线时记录 member_offline 事件并返回 ok=true。
func (r *Room) MarkOffline(token string) (ev Event, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name, exists := r.members[token]
	if !exists || !r.online[token] {
		return Event{}, false
	}
	r.online[token] = false
	payload, _ := json.Marshal(protocol.MemberPayload{Name: name})
	return r.appendLocked(protocol.EvMemberOffline, payload), true
}

// MarkOnline 把指定令牌的成员标记为在线（resume 成功时调用）。
// 仅当成员此前处于离线状态时记录 member_online 事件并返回 changed=true；
// 重复 resume（成员已在线）不会重复产生事件。
func (r *Room) MarkOnline(token string) (ev Event, changed bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	name, exists := r.members[token]
	if !exists || r.online[token] {
		return Event{}, false
	}
	r.online[token] = true
	payload, _ := json.Marshal(protocol.MemberPayload{Name: name})
	return r.appendLocked(protocol.EvMemberOnline, payload), true
}

// Chat 追加一条聊天消息并记录 chat 事件；令牌无效时返回错误。
// 聊天记录只保留最近 DefaultChatKeep 条。
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

// EnsureSession 确保令牌对应的会话存在（用于 resume 时重新绑定成员）。
// 新建的会话初始为离线状态，由调用方随后用 MarkOnline 置为在线。
func (r *Room) EnsureSession(token, name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.members[token]; !ok {
		r.members[token] = name
		r.online[token] = false
	}
}

// Resume 返回序号大于 fromSeq 的事件，供客户端断线续传。
// 若 fromSeq 太旧（缺口事件已被日志截断丢弃），needSnapshot 为 true
// 并返回全量快照代替事件重放。
func (r *Room) Resume(fromSeq uint64) (needSnapshot bool, snap protocol.Snapshot, events []Event) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.log) == 0 {
		if fromSeq < r.seq {
			// 日志为空但序号已前进 —— 缺口无法重放，回退到快照
			return true, protocol.Snapshot{Seq: r.seq, Members: memberNames(r.members), Chats: append([]string(nil), r.chats...)}, nil
		}
		return false, protocol.Snapshot{}, nil
	}
	oldest := r.log[0].Seq
	if fromSeq+1 < oldest {
		// 客户端断点之前的事件已被截断，只能发全量快照
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

// appendLocked 记录一条新事件（调用方必须已持有 r.mu）。
// 日志超出 logCap 时丢弃最旧的事件。
func (r *Room) appendLocked(typ string, payload json.RawMessage) Event {
	r.seq++
	ev := Event{Seq: r.seq, Type: typ, Payload: payload}
	r.log = append(r.log, ev)
	if len(r.log) > r.logCap {
		r.log = r.log[len(r.log)-r.logCap:]
	}
	return ev
}

// memberNames 返回 members 中所有显示名的列表（顺序不定）。
func memberNames(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for _, n := range m {
		out = append(out, n)
	}
	return out
}
