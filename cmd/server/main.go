// quic-room 服务端：基于 QUIC 单流承载自定义 JSON 帧协议的聊天室。
// 每个连接上接受一条流，帧格式见 internal/protocol。
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

// clientConn 表示一个已接入的客户端连接（单条 QUIC 流）。
// mu 保护对该流的并发写，保证帧不交错。
type clientConn struct {
	mu      sync.Mutex
	stream  quic.Stream
	w       io.Writer  // 写端（== stream），独立成字段便于测试替换
	token   string     // 会话令牌（join/resume 后赋值）
	room    *room.Room // 所在房间
	name    string     // 显示名
	lastAck uint64     // 客户端 ack 过的最大事件序号
}

// send 向客户端写入一个帧；加锁保证多 goroutine 广播时不会写交错。
func (c *clientConn) send(typ string, payload any) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return protocol.WriteFrame(c.w, typ, payload)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:4242", "listen address")
	flag.Parse()

	// 每次启动生成新的自签名证书（demo 用，客户端跳过校验）
	cert, err := tlsutil.GenerateSelfSigned()
	if err != nil {
		log.Fatal(err)
	}
	tlsConf := &tls.Config{
		Certificates: []tls.Certificate{cert},
		NextProtos:   []string{"quic-room"}, // ALPN 协议标识
	}
	ln, err := quic.ListenAddr(*addr, tlsConf, &quic.Config{EnableDatagrams: false})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("quic-room server listening on %s", *addr)
	hub := room.NewHub(1024)

	// 主循环：接受 QUIC 连接，每个连接交给独立 goroutine 处理
	for {
		conn, err := ln.Accept(context.Background())
		if err != nil {
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(conn, hub)
	}
}

// handleConn 处理单个客户端连接：接受一条流并循环读帧、分发处理。
// 连接断开时自动让成员离开房间并广播 leave 事件。
func handleConn(conn quic.Connection, hub *room.Hub) {
	defer conn.CloseWithError(0, "bye")
	// 约定：客户端在连接上打开的第一条（双向）流用于协议通信
	stream, err := conn.AcceptStream(context.Background())
	if err != nil {
		return
	}
	defer stream.Close()

	cc := &clientConn{stream: stream, w: stream}
	// 连接结束时：把连接移出广播集合并清理房间成员身份，
	// 在 rs.mu 内向其他人广播离开事件，保证与聊天事件的相对顺序。
	defer func() {
		if cc.room != nil && cc.token != "" {
			rs := stateFor(cc.room)
			rs.mu.Lock()
			ev, ok := cc.room.Leave(cc.token)
			track(cc.room, cc, false)
			if ok {
				broadcast(rs, nil, protocol.TypeEvent, protocol.Event{
					Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload,
				})
			}
			rs.mu.Unlock()
		}
	}()

	// 读帧循环：出错时回送 error 帧，连接断开则返回
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

// roomState 是一个房间的广播状态。
// mu 串行化"房间变更（分配 seq）+ 广播"整个序列：事件的 seq 在房间锁内分配，
// 但发送在房间锁外进行，若不额外串行化，并发广播可能乱序到达客户端，
// 导致客户端记录的 lastSeq 回退、续传重复拉取。同房间的变更+广播必须持有 mu。
type roomState struct {
	mu      sync.Mutex
	clients map[*clientConn]struct{} // 房间内在线连接
}

// roomClients 记录每个房间的广播状态。
var (
	roomClientsMu sync.Mutex
	roomClients   = map[*room.Room]*roomState{}
)

// stateFor 返回房间的广播状态，不存在则创建。
func stateFor(r *room.Room) *roomState {
	roomClientsMu.Lock()
	defer roomClientsMu.Unlock()
	rs := roomClients[r]
	if rs == nil {
		rs = &roomState{clients: map[*clientConn]struct{}{}}
		roomClients[r] = rs
	}
	return rs
}

// track 把连接加入/移出房间的广播集合。
func track(r *room.Room, c *clientConn, add bool) {
	roomClientsMu.Lock()
	defer roomClientsMu.Unlock()
	rs := roomClients[r]
	if rs == nil {
		return // 调用方应先经 stateFor 创建
	}
	if add {
		rs.clients[c] = struct{}{}
	} else {
		delete(rs.clients, c)
		if len(rs.clients) == 0 {
			// 房间没有在线连接时清理映射，避免内存泄漏
			delete(roomClients, r)
		}
	}
}

// broadcast 向房间内除 except 外的所有连接发送一个帧。
// 调用方必须持有 rs.mu，以保证同一房间的事件按 seq 递增顺序送达。
// 发送失败只忽略（连接断开后的清理由 handleConn 的 defer 负责）。
func broadcast(rs *roomState, except *clientConn, typ string, payload any) {
	roomClientsMu.Lock()
	// 先在锁内拷贝连接列表，避免持锁发送阻塞
	clients := make([]*clientConn, 0, len(rs.clients))
	for c := range rs.clients {
		if c != except {
			clients = append(clients, c)
		}
	}
	roomClientsMu.Unlock()
	for _, c := range clients {
		_ = c.send(typ, payload)
	}
}

// dispatch 按帧类型分发处理一个请求；返回非 nil 错误时，
// 调用方会将其作为 error 帧回送给客户端。
func dispatch(cc *clientConn, hub *room.Hub, env protocol.Envelope) error {
	switch env.Type {
	case protocol.TypePing:
		// 心跳：直接回 pong
		return cc.send(protocol.TypePong, nil)
	case protocol.TypeJoin:
		// 加入房间：签发令牌、下发欢迎帧与快照、向其他成员广播 join 事件。
		// 全程持有 rs.mu：保证"分配 seq → 下发 → 广播"与其他事件不交错，
		// 新成员也不会在 Welcome 之前先收到别人的事件。
		var p protocol.Join
		if err := protocol.Decode(env.Payload, &p); err != nil {
			return err
		}
		r := hub.Get(p.Room)
		rs := stateFor(r)
		rs.mu.Lock()
		defer rs.mu.Unlock()
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
		broadcast(rs, cc, protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload})
		return nil
	case protocol.TypeChat:
		// 聊天：在 rs.mu 内"追加事件 + 广播"，保证事件按 seq 顺序送达
		if cc.room == nil {
			return errStr("not joined")
		}
		var p protocol.Chat
		if err := protocol.Decode(env.Payload, &p); err != nil {
			return err
		}
		rs := stateFor(cc.room)
		rs.mu.Lock()
		defer rs.mu.Unlock()
		ev, err := cc.room.Chat(cc.token, p.Text)
		if err != nil {
			return err
		}
		broadcast(rs, nil, protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload})
		return nil
	case protocol.TypeAck:
		// 客户端确认事件序号：仅记录，暂未用于重传
		var p protocol.Ack
		if err := protocol.Decode(env.Payload, &p); err != nil {
			return err
		}
		cc.lastAck = p.Seq
		return nil
	case protocol.TypeResume:
		// 断线续传：凭令牌恢复会话，按 fromSeq 补发事件或下发全量快照。
		// 全程持有 rs.mu：先 track 再补发的窗口期内若有新事件广播，
		// 恢复中的客户端会先收到新事件再收到旧快照/补发事件，造成乱序。
		var p protocol.Resume
		if err := protocol.Decode(env.Payload, &p); err != nil {
			return err
		}
		r := hub.Get(p.Room)
		rs := stateFor(r)
		rs.mu.Lock()
		defer rs.mu.Unlock()
		// demo 约定：令牌内嵌了显示名前缀（"名字-序号"），据此重建成员绑定
		name := p.SessionToken
		if i := indexByte(name, '-'); i >= 0 {
			name = name[:i]
		}
		r.EnsureSession(p.SessionToken, name)
		cc.room, cc.token, cc.name = r, p.SessionToken, name
		track(r, cc, true)
		needSnap, snap, evs := r.Resume(p.FromSeq)
		if needSnap {
			// 事件缺口太大：下发全量快照，客户端直接追平到 snap.Seq
			if err := cc.send(protocol.TypeSnapshot, snap); err != nil {
				return err
			}
		} else {
			// 缺口可重放：逐条补发 fromSeq 之后的事件
			for _, ev := range evs {
				if err := cc.send(protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload}); err != nil {
					return err
				}
			}
		}
		return nil
	default:
		return errStr("unknown type " + env.Type)
	}
}

// strErr 用字符串直接实现的 error，避免引入 errors.New 的样板。
type strErr string

func (e strErr) Error() string { return string(e) }

// errStr 把字符串包装成 error。
func errStr(s string) error { return strErr(s) }

// indexByte 返回字节 c 在 s 中首次出现的下标，找不到返回 -1。
// （等价于 strings.IndexByte，demo 中手写以免多引一个包）
func indexByte(s string, c byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == c {
			return i
		}
	}
	return -1
}

// 占位引用，避免 demo 中暂时未用到的导入报错
var _ = json.Marshal
