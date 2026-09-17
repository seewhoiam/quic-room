package protocol

import (
        "encoding/binary"
        "encoding/json"
        "fmt"
        "io"
)

// WriteFrame writes a length-prefixed JSON envelope.
func WriteFrame(w io.Writer, typ string, payload any) error {
        var raw json.RawMessage
        if payload != nil {
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

// ReadFrame reads one length-prefixed JSON envelope.
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

func Decode[T any](raw json.RawMessage, out *T) error {
        if len(raw) == 0 {
                return fmt.Errorf("empty payload")
        }
        return json.Unmarshal(raw, out)
}
