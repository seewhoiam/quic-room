package main

import (
	"bytes"
	"io"
	"sync"
	"testing"

	"github.com/seewhoiam/quic-room/internal/protocol"
)

// TestFrameWriterConcurrent 回归测试：多个 goroutine 并发写同一流时，
// 帧头与 body 不得交错，对端必须能完整解析出每一帧。
func TestFrameWriterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	out := &frameWriter{w: &buf}

	const writers = 8
	const perWriter = 100

	var wg sync.WaitGroup
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWriter; i++ {
				if err := out.Write(protocol.TypeChat, protocol.Chat{Text: "hello"}); err != nil {
					t.Errorf("write: %v", err)
					return
				}
			}
		}()
	}
	wg.Wait()

	// 所有帧必须能按序完整解析
	n := 0
	for {
		env, err := protocol.ReadFrame(&buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("frame %d corrupted: %v", n, err)
		}
		if env.Type != protocol.TypeChat {
			t.Fatalf("frame %d type=%q want %q", n, env.Type, protocol.TypeChat)
		}
		n++
	}
	if n != writers*perWriter {
		t.Fatalf("parsed %d frames, want %d", n, writers*perWriter)
	}
}
