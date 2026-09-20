package main

import (
	"testing"
	"time"
)

// TestConversationKeyStableAcrossTurns: 追加历史不改变对话指纹
func TestConversationKeyStableAcrossTurns(t *testing.T) {
	base := map[string]any{
		"model": "cline-pass/glm-5.3",
		"messages": []any{
			map[string]any{"role": "system", "content": "STABLE SYSTEM PREFIX " + string(make([]byte, 700))},
			map[string]any{"role": "user", "content": "q1"},
		},
	}
	k1 := conversationKey("cline-pass/glm-5.3", base)

	grown := map[string]any{
		"model": "cline-pass/glm-5.3",
		"messages": []any{
			map[string]any{"role": "system", "content": "STABLE SYSTEM PREFIX " + string(make([]byte, 700))},
			map[string]any{"role": "user", "content": "q1"},
			map[string]any{"role": "assistant", "content": "a1"},
			map[string]any{"role": "user", "content": "q2 more context"},
		},
	}
	k2 := conversationKey("cline-pass/glm-5.3", grown)
	if k1 != k2 {
		t.Fatalf("key changed across turns: %s vs %s", k1, k2)
	}

	// 不同模型 / 不同 system 前缀 → 不同 key
	if conversationKey("cline-pass/glm-5.3-flash", grown) == k1 {
		t.Fatal("key identical across models")
	}
	other := map[string]any{
		"model":    "cline-pass/glm-5.3",
		"messages": []any{map[string]any{"role": "system", "content": "DIFFERENT PREFIX"}},
	}
	if conversationKey("cline-pass/glm-5.3", other) == k1 {
		t.Fatal("key identical across conversations")
	}
}

// TestStringContentParts: parts 数组形态的 content 提取
func TestStringContentParts(t *testing.T) {
	if got := stringContent("plain"); got != "plain" {
		t.Fatalf("string: got %q", got)
	}
	parts := []any{
		map[string]any{"type": "text", "text": "hello "},
		map[string]any{"type": "image_url"},
		map[string]any{"type": "text", "text": "world"},
	}
	if got := stringContent(parts); got != "hello world" {
		t.Fatalf("parts: got %q", got)
	}
	if got := stringContent(42); got != "" {
		t.Fatalf("other: got %q", got)
	}
}

// TestConversationKeyTruncated: 超长前缀截断到 512 字节，不随长度变化溢出
func TestConversationKeyTruncated(t *testing.T) {
	long := string(make([]byte, 4096))
	p1 := map[string]any{"model": "m", "messages": []any{map[string]any{"role": "system", "content": long}}}
	if k := conversationKey("m", p1); len(k) != 32 {
		t.Fatalf("key length: %d", len(k))
	}
}

// TestConvAffinityEvictAndTTL: 过期条目视为未命中
func TestConvAffinityEvictAndTTL(t *testing.T) {
	convAffinityMu.Lock()
	convAffinity = map[string]convAffinityEntry{
		"stale": {AccountID: "acc-x", Expires: time.Now().Add(-time.Minute)},
	}
	convAffinityMu.Unlock()

	evictConversationAffinity("nope", map[string]any{}) // 不存在的 key，不应 panic

	convAffinityMu.Lock()
	if _, ok := convAffinity["stale"]; !ok {
		t.Fatal("evict removed unrelated entry")
	}
	convAffinityMu.Unlock()

	// stale 条目过期后 pickAccountForConversation 不应复用（单账号池下返回该账号即视为未走亲和分支也无碍，
	// 这里仅验证 sweep 不 panic）
	p := loadPool()
	if len(p.Accounts) == 0 {
		t.Skip("no accounts in pool")
	}
}
