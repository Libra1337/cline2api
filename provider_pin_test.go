package main

import "testing"

// TestBuildUpstreamBodyGatewayPin: Vercel/planner 管线模型注入 providerOptions.gateway.only
func TestBuildUpstreamBodyGatewayPin(t *testing.T) {
	for _, model := range []string{"cline-pass/glm-5.3", "glm-5.3"} {
		body := buildUpstreamBody(map[string]any{"model": model}, false)
		gw, ok := body["providerOptions"].(map[string]any)
		if !ok {
			t.Fatalf("model %s: providerOptions missing: %v", model, body)
		}
		inner, ok := gw["gateway"].(map[string]any)
		if !ok {
			t.Fatalf("model %s: providerOptions.gateway missing: %v", model, gw)
		}
		only, _ := inner["only"].([]string)
		if len(only) != 1 || only[0] != "zai" {
			t.Fatalf("model %s: want only=[zai], got %v", model, inner["only"])
		}
		if _, has := body["provider"]; has {
			t.Fatalf("model %s: direct pin leaked into gateway model", model)
		}
	}
}

// TestBuildUpstreamBodyDirectPin: OpenRouter/direct 管线模型注入 provider.only
func TestBuildUpstreamBodyDirectPin(t *testing.T) {
	for _, model := range []string{"cline-pass/glm-5.3-flash", "glm-5.3-flash"} {
		body := buildUpstreamBody(map[string]any{"model": model}, false)
		p, ok := body["provider"].(map[string]any)
		if !ok {
			t.Fatalf("model %s: provider missing: %v", model, body)
		}
		only, _ := p["only"].([]string)
		if len(only) != 1 || only[0] != "z-ai" {
			t.Fatalf("model %s: want only=[z-ai], got %v", model, p["only"])
		}
		if _, has := body["providerOptions"]; has {
			t.Fatalf("model %s: gateway pin leaked into direct model", model)
		}
	}
}

// TestBuildUpstreamBodyClientPinWins: 客户端显式传入时不覆盖（保住 __probe__ 探测通路）
func TestBuildUpstreamBodyClientPinWins(t *testing.T) {
	body := buildUpstreamBody(map[string]any{
		"model":           "cline-pass/glm-5.3",
		"providerOptions": map[string]any{"gateway": map[string]any{"only": []any{"__probe__"}}},
	}, false)
	gw, ok := body["providerOptions"].(map[string]any)
	if !ok {
		t.Fatalf("client providerOptions dropped: %v", body)
	}
	inner, _ := gw["gateway"].(map[string]any)
	only, _ := inner["only"].([]any)
	if len(only) != 1 || only[0] != "__probe__" {
		t.Fatalf("client pin overridden: %v", inner["only"])
	}
	if _, has := body["provider"]; has {
		t.Fatal("table pin injected alongside client pin")
	}
}

// TestBuildUpstreamBodyNoPinForPrivate: 私有通道模型不注入任何 pin
func TestBuildUpstreamBodyNoPinForPrivate(t *testing.T) {
	body := buildUpstreamBody(map[string]any{"model": "cline-pass/deepseek-v4-pro"}, false)
	if _, has := body["providerOptions"]; has {
		t.Fatal("unexpected providerOptions for private-channel model")
	}
	if _, has := body["provider"]; has {
		t.Fatal("unexpected provider for private-channel model")
	}
}

// TestRoutingInfoExtraction: 两条管线的路由结果提取
func TestRoutingInfoExtraction(t *testing.T) {
	vercel := map[string]any{
		"provider_metadata": map[string]any{
			"gateway": map[string]any{
				"routing": map[string]any{"finalProvider": "zai"},
			},
		},
	}
	if pl, prov := routingInfo(vercel); pl != "vercel/planner" || prov != "zai" {
		t.Fatalf("vercel extraction: got %s/%s", pl, prov)
	}

	// finalProvider 藏在 choices[0].message.provider_metadata 内（真实上游形态）
	nested := map[string]any{
		"choices": []any{map[string]any{
			"message": map[string]any{
				"provider_metadata": map[string]any{
					"gateway": map[string]any{
						"routing": map[string]any{"finalProvider": "deepseek"},
					},
				},
			},
		}},
	}
	if pl, prov := routingInfo(nested); pl != "vercel/planner" || prov != "deepseek" {
		t.Fatalf("nested vercel extraction: got %s/%s", pl, prov)
	}

	direct := map[string]any{"provider": "z-ai"}
	if pl, prov := routingInfo(direct); pl != "openrouter/direct" || prov != "z-ai" {
		t.Fatalf("direct extraction: got %s/%s", pl, prov)
	}

	if pl, prov := routingInfo(map[string]any{"id": "x"}); pl != "" || prov != "" {
		t.Fatalf("empty extraction: got %s/%s", pl, prov)
	}
}

// TestLookupProviderPinBoundary: 前缀匹配边界
func TestLookupProviderPinBoundary(t *testing.T) {
	if _, ok := lookupProviderPin("cline-pass/glm-5.3x"); ok {
		t.Fatal("suffix overshoot matched")
	}
	// 任意 "<vendor>/glm-5.3" 都视为 glm-5.3 本体（厂商前缀只影响 pin 前的显示形态，
	// 真实流量实测：客户端发 z-ai/glm-5.3-flash 裸名，漏剥会导致 pin 不命中）
	if _, ok := lookupProviderPin("moonshotai/kimi-k3"); ok {
		t.Fatal("unrelated vendor-prefixed model should not match")
	}
	// deepseek-v4.1-flash 上游原生走 DeepSeek 官方 API，两字段均被忽略，不进 pin 表
	if _, ok := lookupProviderPin("cline-pass/deepseek-v4.1-flash"); ok {
		t.Fatal("deepseek-v4.1-flash should not be pinned (both fields ignored upstream)")
	}
	if pin, ok := lookupProviderPin("cline-pass/glm-5.3"); !ok || pin.slug != "zai" {
		t.Fatalf("glm-5.3 pin: ok=%v pin=%+v", ok, pin)
	}
	if pin, ok := lookupProviderPin("cline-free/muse-spark-1.3-contributor"); !ok || pin.field != "direct" || pin.slug != "meta" {
		t.Fatalf("muse pin: ok=%v pin=%+v", ok, pin)
	}
	// 厂商前缀裸名（客户端实际发送形态）：z-ai/glm-5.3-flash 必须命中 direct/z-ai pin，
	// 漏剥会导致默认路由打散缓存
	if pin, ok := lookupProviderPin("z-ai/glm-5.3-flash"); !ok || pin.field != "direct" || pin.slug != "z-ai" {
		t.Fatalf("vendor-prefix pin miss: ok=%v pin=%+v", ok, pin)
	}
	if pin, ok := lookupProviderPin("deepseek/glm-5.3"); !ok || pin.slug != "zai" {
		t.Fatalf("generic suffix pin miss: ok=%v pin=%+v", ok, pin)
	}
	if _, ok := lookupProviderPin("moonshotai/kimi-k3"); ok {
		t.Fatal("unrelated vendor-prefixed model should not match")
	}
	if pin, ok := lookupProviderPin("cline-pass/mimo-v2.5"); !ok || pin.field != "direct" || pin.slug != "xiaomi" {
		t.Fatalf("mimo pin: ok=%v pin=%+v", ok, pin)
	}
	if pin, ok := lookupProviderPin("xiaomi/mimo-v2.5-pro"); !ok || pin.slug != "xiaomi" {
		t.Fatalf("mimo-pro vendor-prefix pin: ok=%v pin=%+v", ok, pin)
	}
	if _, ok := lookupProviderPin("mimo-v2.5-free"); ok {
		t.Fatal("free variant is a distinct model, should not inherit pin")
	}
}

// TestBumpMaxTokensForRetry: 空响应重试的预算提升规则
func TestBumpMaxTokensForRetry(t *testing.T) {
	cases := []struct {
		in   any
		want float64
		ok   bool
	}{
		{float64(128), 512, true},
		{float64(512), 2048, true},
		{float64(3000), 8192, true}, // ×4 超封顶 → 8192
		{float64(8192), 0, false},   // 已达封顶不重试
		{float64(0), 0, false},      // 未设置
		{nil, 0, false},             // 无字段
		{"512", 0, false},           // 非数值
	}
	for _, c := range cases {
		params := map[string]any{}
		if c.in != nil {
			params["max_tokens"] = c.in
		}
		ok := bumpMaxTokensForRetry(params)
		if ok != c.ok {
			t.Fatalf("in=%v ok=%v want %v", c.in, ok, c.ok)
		}
		if ok && params["max_tokens"].(float64) != c.want {
			t.Fatalf("in=%v bumped to %v want %v", c.in, params["max_tokens"], c.want)
		}
	}
}

// TestMaybeAliasToClinePass: 裸名付费 zen 模型回退 Cline Pass 同名模型
func TestMaybeAliasToClinePass(t *testing.T) {
	// 无 zen 记录的模型（走 cline 池）不应触发别名
	if alt := maybeAliasToClinePass("cline-pass/deepseek-v4.1-flash"); alt != "" {
		t.Fatalf("cline-pass model should not alias, got %s", alt)
	}
	if alt := maybeAliasToClinePass(""); alt != "" {
		t.Fatalf("empty model should not alias, got %s", alt)
	}
	// 模型目录里存在 cline-pass/deepseek-v4.1-flash 时，付费 zen 裸名应回退到它
	// （模型目录来自远程同步，测试环境无网络时 getAllModels 回退内置表，此断言只在目录包含该模型时生效）
	for _, m := range getAllModels() {
		if m.ID == "cline-pass/deepseek-v4.1-flash" {
			if alt := maybeAliasToClinePass("deepseek-v4.1-flash"); alt != "cline-pass/deepseek-v4.1-flash" {
				t.Fatalf("alias mismatch: got %q", alt)
			}
			return
		}
	}
	t.Log("model catalog lacks cline-pass/deepseek-v4.1-flash (offline fallback); alias assertion skipped")
}
