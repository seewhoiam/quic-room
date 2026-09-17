package protocol

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// WriteFrame 写入一个带长度前缀的 JSON 帧：
// 4 字节大端 uint32 表示 body 长度，随后是 JSON 编码的 Envelope。
// payload 为 nil 时只发送 Type。
func WriteFrame(w io.Writer, typ string, payload any) error {
	var raw json.RawMessage
	if payload != nil {
		// 先把载荷编码为 RawMessage，再嵌入外层 Envelope
		b, err := json.Marshal(payload)
		if err != nil {
			return err
		}
		raw = b
	}
	env := Envelope{Type: typ, Payload: raw}
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(body)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(body)
	return err
}

// ReadFrame 读取一个带长度前缀的 JSON 帧。
// 帧体大小限制在 1 字节到 1 MiB 之间，超出返回错误。
func ReadFrame(r io.Reader) (Envelope, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return Envelope{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 || n > 1<<20 {
		return Envelope{}, fmt.Errorf("invalid frame size %d", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return Envelope{}, err
	}
	var env Envelope
	if err := json.Unmarshal(buf, &env); err != nil {
		return Envelope{}, err
	}
	return env, nil
}

// Decode 把 Envelope 中的 RawMessage 载荷解码到具体类型 out。
// 空载荷（如 ping/pong）会返回错误。
func Decode[T any](raw json.RawMessage, out *T) error {
	if len(raw) == 0 {
		return fmt.Errorf("empty payload")
	}
	return json.Unmarshal(raw, out)
}
