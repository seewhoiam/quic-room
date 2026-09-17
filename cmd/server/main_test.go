package main

import (
	"bytes"
	"encoding/json"
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

// mkEnv 构造一个测试用的请求帧。
func mkEnv(t *testing.T, typ string, payload any) protocol.Envelope {
	t.Helper()
	b, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return protocol.Envelope{Type: typ, Payload: b}
}

// readAll 把 buffer 中的帧全部解析出来。
func readAll(t *testing.T, buf *bytes.Buffer) []protocol.Envelope {
	t.Helper()
	var out []protocol.Envelope
	for {
		env, err := protocol.ReadFrame(buf)
		if err == io.EOF {
			return out
		}
		if err != nil {
			t.Fatalf("read frame: %v", err)
		}
		out = append(out, env)
	}
}

// TestResumeOfflineMemberGetsOnlineAndResumeOk 回归测试：
// 离线成员 resume 回来时，应广播 member_online（含自己）并收到 resume_ok 应答。
func TestResumeOfflineMemberGetsOnlineAndResumeOk(t *testing.T) {
	hub := room.NewHub(16)

	// alice 先 join
	var buf1 bytes.Buffer
	cc1 := &clientConn{w: &buf1}
	if err := dispatch(cc1, hub, mkEnv(t, protocol.TypeJoin, protocol.Join{Room: "demo", Name: "alice"})); err != nil {
		t.Fatal(err)
	}
	frames := readAll(t, &buf1)
	if len(frames) != 1 || frames[0].Type != protocol.TypeWelcome {
		t.Fatalf("want 1 welcome frame, got %v", frames)
	}
	var w protocol.Welcome
	if err := protocol.Decode(frames[0].Payload, &w); err != nil {
		t.Fatal(err)
	}

	// alice 断连 → 离线
	if _, ok := hub.Get("demo").MarkOffline(w.SessionToken); !ok {
		t.Fatal("MarkOffline should succeed")
	}

	// alice 重连 resume：应收到 member_online + resume_ok
	var buf2 bytes.Buffer
	cc2 := &clientConn{w: &buf2}
	err := dispatch(cc2, hub, mkEnv(t, protocol.TypeResume, protocol.Resume{
		Room: "demo", FromSeq: w.SnapshotSeq, SessionToken: w.SessionToken,
	}))
	if err != nil {
		t.Fatal(err)
	}
	frames = readAll(t, &buf2)
	// 补发列表里有她自己的 member_offline（离线期间发生），之后是 member_online 和 resume_ok
	if len(frames) != 3 {
		t.Fatalf("want 3 frames (offline replay, online, resume_ok), got %d", len(frames))
	}
	var ev protocol.Event
	if err := protocol.Decode(frames[0].Payload, &ev); err != nil || ev.Type != protocol.EvMemberOffline {
		t.Fatalf("frame0 should replay member_offline, got %v", frames[0])
	}
	if err := protocol.Decode(frames[1].Payload, &ev); err != nil || ev.Type != protocol.EvMemberOnline {
		t.Fatalf("frame1 should be member_online, got %v", frames[1])
	}
	if frames[2].Type != protocol.TypeResumeOk {
		t.Fatalf("frame2 should be resume_ok, got %q", frames[2].Type)
	}
	var ro protocol.ResumeOk
	if err := protocol.Decode(frames[2].Payload, &ro); err != nil {
		t.Fatal(err)
	}
	if ro.Seq != ev.Seq {
		t.Fatalf("resume_ok seq=%d want %d (caught up to online event)", ro.Seq, ev.Seq)
	}
}

// TestResumeUpToDateStillGetsResumeOk 回归测试：
// 即使没有新事件可补发（fromSeq 已是最新），resume 也必须收到 resume_ok，
// 客户端据此区分"续传成功"与"请求丢失"。
func TestResumeUpToDateStillGetsResumeOk(t *testing.T) {
	hub := room.NewHub(16)

	var buf1 bytes.Buffer
	cc1 := &clientConn{w: &buf1}
	if err := dispatch(cc1, hub, mkEnv(t, protocol.TypeJoin, protocol.Join{Room: "demo", Name: "alice"})); err != nil {
		t.Fatal(err)
	}
	var w protocol.Welcome
	_ = protocol.Decode(readAll(t, &buf1)[0].Payload, &w)

	// 成员仍在线、fromSeq 已最新：应只收到一个 resume_ok，无事件、无快照
	var buf2 bytes.Buffer
	cc2 := &clientConn{w: &buf2}
	err := dispatch(cc2, hub, mkEnv(t, protocol.TypeResume, protocol.Resume{
		Room: "demo", FromSeq: w.SnapshotSeq, SessionToken: w.SessionToken,
	}))
	if err != nil {
		t.Fatal(err)
	}
	frames := readAll(t, &buf2)
	if len(frames) != 1 || frames[0].Type != protocol.TypeResumeOk {
		t.Fatalf("want exactly 1 resume_ok frame, got %v", frames)
	}
	var ro protocol.ResumeOk
	if err := protocol.Decode(frames[0].Payload, &ro); err != nil {
		t.Fatal(err)
	}
	if ro.Seq != w.SnapshotSeq {
		t.Fatalf("resume_ok seq=%d want %d", ro.Seq, w.SnapshotSeq)
	}
}
