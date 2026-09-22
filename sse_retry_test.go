package main

import (
	"io"
	"strings"
	"testing"
)

func readAll(t *testing.T, r io.Reader) string {
	t.Helper()
	b, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("readall: %v", err)
	}
	return string(b)
}

// TestPeekSSEFirstEventData: 正常流——首事件 payload 提取，剩余内容无损拼回
func TestPeekSSEFirstEventData(t *testing.T) {
	raw := ": keepalive\n\ndata: {\"id\":\"x\",\"choices\":[]}\n\ndata: {\"second\":true}\n\n"
	body := io.NopCloser(strings.NewReader(raw))
	payload, combined := peekSSEFirstEvent(body)
	if string(payload) != `{"id":"x","choices":[]}` {
		t.Fatalf("payload=%q", payload)
	}
	rest := readAll(t, combined)
	if rest != raw {
		t.Fatalf("combined mismatch:\n got %q\nwant %q", rest, raw)
	}
}

// TestPeekSSEFirstEventError: 错误事件判定（含注释前缀）
func TestPeekSSEFirstEventError(t *testing.T) {
	raw := "data: {\"error\":{\"code\":\"stream_initialization_failed\",\"message\":\"boom\"}}\n\n"
	_, combined := peekSSEFirstEvent(io.NopCloser(strings.NewReader(raw)))
	_ = combined
	if !sseErrorPayload([]byte(`{"error":{"code":"x"}}`)) {
		t.Fatal("error event not detected")
	}
	if sseErrorPayload([]byte(`{"id":"x","choices":[]}`)) {
		t.Fatal("normal event misdetected as error")
	}
	if sseErrorPayload([]byte(`[DONE]`)) || sseErrorPayload(nil) || sseErrorPayload([]byte("not json")) {
		t.Fatal("non-json / DONE misdetected")
	}
}

// TestPeekSSEFirstEventEOF: 上游直接 EOF（连接早断）不 panic，已读字节拼回
func TestPeekSSEFirstEventEOF(t *testing.T) {
	raw := "garbage without newline"
	payload, combined := peekSSEFirstEvent(io.NopCloser(strings.NewReader(raw)))
	if payload != nil {
		t.Fatalf("expected nil payload, got %q", payload)
	}
	if rest := readAll(t, combined); rest != raw {
		t.Fatalf("combined mismatch: %q", rest)
	}
}

// TestPrefixBodyClosePropagation: Close 传导到原始 body
func TestPrefixBodyClosePropagation(t *testing.T) {
	inner := &closerCounter{Reader: strings.NewReader("rest")}
	pb := &prefixBody{prefix: []byte("pre"), rest: &bodyWithClose{r: inner, body: inner}}
	buf := make([]byte, 3)
	pb.Read(buf)
	if string(buf) != "pre" {
		t.Fatalf("prefix=%q", buf)
	}
	if err := pb.Close(); err != nil {
		t.Fatal(err)
	}
	if !inner.closed {
		t.Fatal("Close not propagated to inner body")
	}
}

type closerCounter struct {
	io.Reader
	closed bool
}

func (c *closerCounter) Close() error { c.closed = true; return nil }
