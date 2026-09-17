package main

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/seewhoiam/quic-room/internal/protocol"
	"github.com/seewhoiam/quic-room/internal/room"
)

// TestBroadcastOrdered 回归测试：多个 goroutine 并发"聊天 + 广播"时，
// 每个客户端收到的事件 seq 必须严格递增（不重复、不回退）。
// 该不变量是断线续传（lastSeq 对齐）的前提。
func TestBroadcastOrdered(t *testing.T) {
	hub := room.NewHub(4096)
	r := hub.Get("demo")
	rs := stateFor(r)

	// 两名成员发言 + 两名接收者
	var senderToks []string
	for _, name := range []string{"alice", "bob"} {
		tok, _, _, err := r.Join(name)
		if err != nil {
			t.Fatal(err)
		}
		senderToks = append(senderToks, tok)
	}

	// 假客户端：写端指向各自的 buffer（clientConn.send 内部按连接加锁）
	bufs := make([]*bytes.Buffer, 2)
	for i := range bufs {
		bufs[i] = &bytes.Buffer{}
		track(r, &clientConn{w: bufs[i]}, true)
	}

	const rounds = 200
	var wg sync.WaitGroup
	for _, tok := range senderToks {
		wg.Add(1)
		go func(tok string) {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				// 与 dispatch 的 TypeChat 分支保持一致：变更 + 广播在 rs.mu 内完成
				rs.mu.Lock()
				ev, err := r.Chat(tok, "x")
				if err != nil {
					t.Errorf("chat: %v", err)
					rs.mu.Unlock()
					return
				}
				broadcast(rs, nil, protocol.TypeEvent, protocol.Event{Seq: ev.Seq, Type: ev.Type, Payload: ev.Payload})
				rs.mu.Unlock()
			}
		}(tok)
	}
	wg.Wait()

	// 每个客户端收到的事件 seq 必须严格递增
	for i, buf := range bufs {
		var prev uint64
		n := 0
		for {
			env, err := protocol.ReadFrame(buf)
			if err == io.EOF {
				break
			}
			if err != nil {
				t.Fatalf("client %d frame %d corrupted: %v", i, n, err)
			}
			var e protocol.Event
			if err := protocol.Decode(env.Payload, &e); err != nil {
				t.Fatalf("client %d frame %d decode: %v", i, n, err)
			}
			if e.Seq <= prev {
				t.Fatalf("client %d: seq not increasing: prev=%d got=%d", i, prev, e.Seq)
			}
			prev = e.Seq
			n++
		}
		if n != rounds*len(senderToks) {
			t.Fatalf("client %d got %d events, want %d", i, n, rounds*len(senderToks))
		}
	}
}
