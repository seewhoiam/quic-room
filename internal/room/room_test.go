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
