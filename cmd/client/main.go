// quic-room 客户端：命令行交互式聊天，支持断线续传。
// 会话（令牌 + 已收序号）持久化在系统临时目录，重启后自动 resume。
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/seewhoiam/quic-room/internal/protocol"
)

// sessionFile 是持久化到磁盘的会话状态，用于进程重启后的断线续传。
type sessionFile struct {
	Token   string `json:"token"`   // 服务端签发的会话令牌
	LastSeq uint64 `json:"lastSeq"` // 已收到的最大事件序号
	Room    string `json:"room"`
	Name    string `json:"name"`
}

// frameWriter 串行化对同一流/Writer 的帧写入。
// ack（读循环）、ping（心跳 goroutine）、chat（主 goroutine）来自不同 goroutine，
// 若不加锁，4 字节帧头与 body 可能交错写入，导致对端 ReadFrame 解析错乱。
type frameWriter struct {
	mu sync.Mutex
	w  io.Writer
}

// Write 原子地写入一个完整帧。
func (f *frameWriter) Write(typ string, payload any) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return protocol.WriteFrame(f.w, typ, payload)
}

func main() {
	addr := flag.String("addr", "127.0.0.1:4242", "server address")
	roomName := flag.String("room", "demo", "room name")
	name := flag.String("name", "anon", "display name")
	insecure := flag.Bool("insecure", true, "skip TLS verify (local demo)")
	flag.Parse()

	// 本地 demo 使用自签名证书，默认跳过证书校验
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

	// 打开一条双向流承载协议帧；所有写操作都必须经过 out（带锁）
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		log.Fatal(err)
	}
	out := &frameWriter{w: stream}

	// 会话文件按 房间+名字 区分，存放在系统临时目录
	sessPath := filepath.Join(os.TempDir(), fmt.Sprintf("quic-room-%s-%s.session", *roomName, *name))
	var lastSeq uint64
	var token string
	// 读取上次保存的会话；房间或名字不匹配则忽略
	if b, err := os.ReadFile(sessPath); err == nil {
		var s sessionFile
		if json.Unmarshal(b, &s) == nil && s.Room == *roomName && s.Name == *name && s.Token != "" {
			token, lastSeq = s.Token, s.LastSeq
		}
	}

	// 有旧会话则 resume 续传，否则走 join 加入
	if token != "" {
		if err := out.Write(protocol.TypeResume, protocol.Resume{
			Room: *roomName, FromSeq: lastSeq, SessionToken: token,
		}); err != nil {
			log.Fatal(err)
		}
		log.Printf("resume room=%s fromSeq=%d", *roomName, lastSeq)
	} else {
		if err := out.Write(protocol.TypeJoin, protocol.Join{Room: *roomName, Name: *name}); err != nil {
			log.Fatal(err)
		}
	}

	// 读循环：接收服务端推送的帧，更新本地会话进度并打印
	go func() {
		for {
			env, err := protocol.ReadFrame(stream)
			if err != nil {
				log.Printf("disconnected: %v", err)
				os.Exit(0)
			}
			switch env.Type {
			case protocol.TypeWelcome:
				// join 成功：保存令牌与快照序号
				var w protocol.Welcome
				_ = protocol.Decode(env.Payload, &w)
				token = w.SessionToken
				lastSeq = w.SnapshotSeq
				saveSession(sessPath, *roomName, *name, token, lastSeq)
				fmt.Printf("[welcome] members=%v chats=%v seq=%d\n", w.Snapshot.Members, w.Snapshot.Chats, w.SnapshotSeq)
			case protocol.TypeSnapshot:
				// resume 缺口太大收到全量快照：直接以快照序号为准
				var s protocol.Snapshot
				_ = protocol.Decode(env.Payload, &s)
				lastSeq = s.Seq
				saveSession(sessPath, *roomName, *name, token, lastSeq)
				fmt.Printf("[snapshot] members=%v chats=%v seq=%d\n", s.Members, s.Chats, s.Seq)
			case protocol.TypeResumeOk:
				// 续传完成确认：以服务端告知的序号为准落盘
				// （即使没有补发任何事件也会收到，表示续传成功）
				var ro protocol.ResumeOk
				_ = protocol.Decode(env.Payload, &ro)
				lastSeq = ro.Seq
				saveSession(sessPath, *roomName, *name, token, lastSeq)
				fmt.Printf("[resume_ok] caught up seq=%d\n", ro.Seq)
			case protocol.TypeEvent:
				// 普通事件：推进序号、落盘、回 ack
				var e protocol.Event
				_ = protocol.Decode(env.Payload, &e)
				lastSeq = e.Seq
				saveSession(sessPath, *roomName, *name, token, lastSeq)
				_ = out.Write(protocol.TypeAck, protocol.Ack{Seq: e.Seq})
				fmt.Printf("[event %d %s] %s\n", e.Seq, e.Type, string(e.Payload))
			case protocol.TypeError:
				fmt.Printf("[error] %s\n", string(env.Payload))
			case protocol.TypePong:
				// 心跳应答，忽略
			case protocol.TypeBye:
				fmt.Printf("[bye] %s\n", string(env.Payload))
			default:
				fmt.Printf("[%s] %s\n", env.Type, string(env.Payload))
			}
		}
	}()

	// 心跳：每 15 秒发一次 ping，保持连接活跃
	go func() {
		t := time.NewTicker(15 * time.Second)
		defer t.Stop()
		for range t.C {
			_ = out.Write(protocol.TypePing, nil)
		}
	}()

	// 主 goroutine：从标准输入读聊天内容并发送
	sc := bufio.NewScanner(os.Stdin)
	fmt.Println("type chat and enter; Ctrl+C to quit (session saved for resume)")
	for sc.Scan() {
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		if err := out.Write(protocol.TypeChat, protocol.Chat{Text: text}); err != nil {
			log.Fatal(err)
		}
	}
}

// saveSession 把会话状态写入磁盘（仅属主可读写），供下次启动续传。
func saveSession(path, roomName, name, token string, seq uint64) {
	b, _ := json.Marshal(sessionFile{Token: token, LastSeq: seq, Room: roomName, Name: name})
	_ = os.WriteFile(path, b, 0o600)
}
