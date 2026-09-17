package room

import "testing"

func TestResumeWithinWindow(t *testing.T) {
	r := newRoom("demo", 8)
	tok, _, _, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err := r.Chat(tok, "hi"); err != nil {
			t.Fatal(err)
		}
	}
	need, _, evs := r.Resume(1) // after join
	if need {
		t.Fatalf("expected catch-up, got snapshot")
	}
	if len(evs) != 3 {
		t.Fatalf("want 3 events, got %d", len(evs))
	}
	if evs[0].Seq != 2 {
		t.Fatalf("first seq=%d want 2", evs[0].Seq)
	}
}

func TestResumeTooOldNeedsSnapshot(t *testing.T) {
	r := newRoom("demo", 3)
	tok, _, _, _ := r.Join("alice")
	for i := 0; i < 5; i++ {
		_, _ = r.Chat(tok, "x")
	}
	// log cap 3 → oldest seq > 1
	need, snap, evs := r.Resume(1)
	if !need {
		t.Fatalf("expected snapshot, got %d events", len(evs))
	}
	if snap.Seq == 0 {
		t.Fatal("empty snapshot seq")
	}
}

// 断连只标记离线、成员关系保留；重复标记是幂等的。
func TestMarkOfflineKeepsMembership(t *testing.T) {
	r := newRoom("demo", 8)
	tok, _, _, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}

	ev, ok := r.MarkOffline(tok)
	if !ok {
		t.Fatal("first MarkOffline should succeed")
	}
	if ev.Type != "member_offline" {
		t.Fatalf("event type=%q want member_offline", ev.Type)
	}

	// 离线后成员仍在房间内（快照可见、聊天不被移除）
	if _, exists := r.members[tok]; !exists {
		t.Fatal("member should be kept after going offline")
	}
	snap := r.Snapshot()
	if len(snap.Members) != 1 || snap.Members[0] != "alice" {
		t.Fatalf("snapshot members=%v want [alice]", snap.Members)
	}

	// 重复 MarkOffline 幂等：不再产生事件
	if _, ok := r.MarkOffline(tok); ok {
		t.Fatal("second MarkOffline should be no-op")
	}
	// 未知令牌
	if _, ok := r.MarkOffline("nobody-99"); ok {
		t.Fatal("MarkOffline for unknown token should fail")
	}
}

// 离线成员 resume 回来标记上线并产生 member_online；重复上线幂等。
func TestMarkOnlineAfterOffline(t *testing.T) {
	r := newRoom("demo", 8)
	tok, _, _, err := r.Join("alice")
	if err != nil {
		t.Fatal(err)
	}

	// 在线时 MarkOnline 是 no-op
	if _, changed := r.MarkOnline(tok); changed {
		t.Fatal("MarkOnline on online member should be no-op")
	}

	if _, ok := r.MarkOffline(tok); !ok {
		t.Fatal("MarkOffline should succeed")
	}
	ev, changed := r.MarkOnline(tok)
	if !changed {
		t.Fatal("MarkOnline after offline should report change")
	}
	if ev.Type != "member_online" {
		t.Fatalf("event type=%q want member_online", ev.Type)
	}

	// online 事件已进入事件日志，resume 可以重放到
	need, _, evs := r.Resume(ev.Seq - 1)
	if need {
		t.Fatal("should replay, not snapshot")
	}
	if len(evs) != 1 || evs[0].Type != "member_online" {
		t.Fatalf("replay=%v want [member_online]", evs)
	}
}

// EnsureSession 新建的会话初始为离线，随后 MarkOnline 置为在线。
func TestEnsureSessionStartsOffline(t *testing.T) {
	r := newRoom("demo", 8)
	r.EnsureSession("bob-7", "bob")
	if r.online["bob-7"] {
		t.Fatal("new session should start offline")
	}
	ev, changed := r.MarkOnline("bob-7")
	if !changed {
		t.Fatal("MarkOnline after EnsureSession should report change")
	}
	if ev.Type != "member_online" {
		t.Fatalf("event type=%q want member_online", ev.Type)
	}
	// EnsureSession 不覆盖已存在的成员名
	r.EnsureSession("bob-7", "mallory")
	if name := r.members["bob-7"]; name != "bob" {
		t.Fatalf("name=%q want bob (must not be overwritten)", name)
	}
}
