package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultMaxTokens       = 128000
	defaultReasoningEffort = "high"
	fallbackDefaultModel   = "z-ai/glm-5.3-flash"
	freeModelPrimary       = "z-ai/glm-5.3-flash"
	freeModelFallback      = "deepseek/deepseek-v4-flash"
	freeModelLastResort    = "cline-free/longcat-2.0"
)

// freeModelChain 是 model="free" 时的降级顺序。
// 顺序依据 Artificial Analysis Intelligence Index v4.1.1：
// glm-5.3-flash 57 > deepseek-v4-flash 0731 52 > longcat-2.0 34。
var freeModelChain = []string{freeModelPrimary, freeModelFallback, freeModelLastResort}

// builtinModels 是内置默认模型列表（不可删除），仅作为离线 / 未同步时的 fallback。
// 同步 Cline 官方推荐模型成功后，getAllModels 以远程模型为主。
var builtinModels = []Model{
	{ID: "z-ai/glm-5.3-flash", Provider: "z-ai", Cost: "free", Status: "active", Custom: false},
	{ID: "cline-free/longcat-2.0", Provider: "cline-free", Cost: "free", Status: "active", Custom: false},
	{ID: "cline-pass/glm-5.2", Provider: "zai", Cost: "pass", Status: "active", Custom: false},
	{ID: "cline-pass/deepseek-v4-flash", Provider: "deepseek", Cost: "pass", Status: "active", Custom: false},
	{ID: "cline-pass/qwen3.7-max", Provider: "qwen", Cost: "pass", Status: "active", Custom: false},
	{ID: "deepseek/deepseek-v4-flash", Provider: "deepseek", Cost: "free", Status: "active", Custom: false},
	{ID: "poolside/laguna-s-2.1:free", Provider: "poolside", Cost: "free", Status: "active", Custom: false},
}

// getAllModels 返回可用模型列表：
//   - 已同步远程模型：Cline 远程（Source=remote）+ opencode 同步（Source=zen）+ 用户自定义
//   - 未同步 / 离线：内置 fallback（Cline + zen 种子表）+ 用户自定义
func getAllModels() []Model {
	p := loadPool()
	poolMu.Lock()
	defer poolMu.Unlock()

	var custom []Model
	var remote []Model
	var zen []Model
	for _, m := range p.Models {
		switch m.Source {
		case "remote":
			remote = append(remote, m)
		case "zen":
			zen = append(zen, m)
		default:
			custom = append(custom, m)
		}
	}

	if len(remote) > 0 || len(zen) > 0 || remoteZenActive() {
		result := make([]Model, 0, len(remote)+len(zen)+len(custom))
		result = append(result, remote...)
		result = append(result, zen...)
		result = append(result, custom...)
		return result
	}

	builtin := make([]Model, 0, len(builtinModels)+len(zenSeedModels))
	builtin = append(builtin, builtinModels...)
	builtin = append(builtin, builtinZenModels()...)

	result := make([]Model, 0, len(builtin)+len(custom))
	result = append(result, builtin...)
	result = append(result, custom...)
	return result
}

// getDefaultModel 返回用户设置的默认模型；未设置时优先回退到第一个远程 free 模型，
// 否则用内置 fallback。
func getDefaultModel() string {
	p := loadPool()
	poolMu.Lock()
	defer poolMu.Unlock()

	if p.DefaultModel != "" {
		return p.DefaultModel
	}

	for _, m := range p.Models {
		if m.Source == "remote" && m.Cost == "free" {
			return m.ID
		}
	}

	for _, m := range builtinModels {
		if m.Cost == "free" {
			return m.ID
		}
	}
	return fallbackDefaultModel
}

// 当前监听地址（startProxy 启动时赋值，供管理后台展示）。
var (
	listenHost string
	listenPort int
)

// HTTP server 实例与路由表（restartListener 换地址重启时复用）。
var (
	serverMux     *http.ServeMux
	currentServer *http.Server
	serverMu      sync.Mutex
)

// restartListener 用新地址重启 HTTP 监听。
// 注意：必须在 goroutine 中调用——Shutdown 会等待当前 HTTP 请求完成，
// 若在 admin handler 内同步调用会死锁。
func restartListener(host string, port int) error {
	if host == "" {
		host = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	listenHost = host
	listenPort = port

	serverMu.Lock()
	old := currentServer
	server := &http.Server{Addr: addr, Handler: serverMux}
	currentServer = server
	serverMu.Unlock()

	if old != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = old.Shutdown(ctx)
		cancel()
	}

	fmt.Println("")
	fmt.Println(strings.Repeat("=", 58))
	fmt.Printf("  Listener restarted: %s\n", addr)
	if !isLoopbackHost(host) {
		for _, ip := range detectLocalIPs() {
			fmt.Printf("  http://%s:%d (LAN)\n", ip, port)
		}
		fmt.Println("  !!! 监听非本机地址，管理后台无鉴权，请确认网络环境安全")
	}
	fmt.Println(strings.Repeat("=", 58))
	return server.ListenAndServe()
}

// effectiveAdminHost 返回管理后台/浏览器实际可用的访问地址：
// host 为空或通配地址（0.0.0.0 / ::）时展示回环 127.0.0.1，否则返回 host 本身。
func effectiveAdminHost(host string) string {
	switch host {
	case "", "0.0.0.0", "::":
		return "127.0.0.1"
	}
	return host
}

// detectLocalIPs 检测本机所有可用 IPv4 地址（排除回环、链路本地和未启用的网卡）。
func detectLocalIPs() []string {
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	result := make([]string, 0, len(addrs))
	for _, a := range addrs {
		ipNet, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip := ipNet.IP
		if ip == nil || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
			continue
		}
		if v4 := ip.To4(); v4 != nil {
			result = append(result, v4.String())
		}
	}
	return result
}

// isLoopbackHost 判断监听地址是否为回环（127.x / localhost），用于安全提示。
func isLoopbackHost(host string) bool {
	switch host {
	case "", "localhost", "127.0.0.1":
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

var passThroughKeys = []string{
	"tools", "tool_choice", "parallel_tool_calls", "functions", "function_call",
	"temperature", "top_p", "top_k", "stop", "presence_penalty", "frequency_penalty",
	"response_format", "user", "n", "logit_bias", "seed", "logprobs", "top_logprobs",
	"stream_options", "metadata",
	// Provider 固定字段：客户端显式传入时透传上游（也用于 __probe__ 管线探测）
	"providerOptions", "provider",
}

// providerPin 描述把一个 Cline Pass 模型固定到官方上游的方式。
// Cline Pass 有两条路由管线，pin 字段不同且写错会被上游静默忽略（请求照常成功）：
//   - Vercel / planner 管线：providerOptions.gateway.only
//   - OpenRouter / direct 管线：provider.only
// Z.AI 在两条管线里的 slug 不同：Vercel=zai，OpenRouter=z-ai，不可混用。
type providerPin struct {
	field string // "gateway" = providerOptions.gateway.only | "direct" = provider.only
	slug  string // 上游 provider slug
}

// modelProviderPins 模型 → 官方上游固定映射。key 为去掉 cline-pass/ 前缀后的模型 ID。
// 实测（__probe__ 双字段探测 + finalProvider 验证，2026-09-20）：
//   - glm-5.3：Vercel 管线，gateway.only 生效，finalProvider=zai
//   - glm-5.3-flash：OpenRouter 管线，provider.only 生效，响应顶层 provider=Z.AI
//   - deepseek-v4.1-flash：两字段均被忽略 —— 上游原生走 DeepSeek 官方 API
//     （响应带 provider_metadata.deepseek.promptCache* 官方特征），无需也无法 pin
//   - cline-pass/deepseek-v4-pro：openai-compatible-private 私有通道（唯一出口），无需 pin
//   - muse-spark-1.3-contributor：OpenRouter 管线，当前唯一 provider=meta（官方）。
//     今天 pin 是 no-op，但若未来 OpenRouter 加第三方 provider，默认路由会悄悄打散缓存——固定住。
//     实测：缓存预热慢（3+ 发）+ 随机驱逐；输出需 max_tokens≥512（加密思维链烧预算）。
var modelProviderPins = map[string]providerPin{
	"glm-5.3":                    {field: "gateway", slug: "zai"},
	"glm-5.3-flash":              {field: "direct", slug: "z-ai"},
	"muse-spark-1.3-contributor": {field: "direct", slug: "meta"},
}

// lookupProviderPin 按完整模型 ID 或去掉 cline-pass/ / cline-free/ 前缀后的 ID 查 pin。
func lookupProviderPin(model string) (providerPin, bool) {
	if pin, ok := modelProviderPins[model]; ok {
		return pin, true
	}
	for _, prefix := range []string{"cline-pass/", "cline-free/"} {
		if suffix, found := strings.CutPrefix(model, prefix); found {
			if pin, ok := modelProviderPins[suffix]; ok {
				return pin, true
			}
		}
	}
	return providerPin{}, false
}

// routingInfo 从上游响应中提取实际路由结果（在 normalizeOpenAIResponse 剥离 metadata 之前调用）：
//   - Vercel / planner 管线：provider_metadata.gateway.routing.finalProvider（顶层或 choices[0].message 内）
//   - OpenRouter / direct 管线：响应顶层 provider 字段
func routingInfo(obj map[string]any) (pipeline, provider string) {
	candidates := []map[string]any{obj}
	if choices, ok := obj["choices"].([]any); ok && len(choices) > 0 {
		if c, ok := choices[0].(map[string]any); ok {
			if msg, ok := c["message"].(map[string]any); ok {
				candidates = append(candidates, msg)
			}
			if delta, ok := c["delta"].(map[string]any); ok {
				candidates = append(candidates, delta)
			}
		}
	}
	for _, cand := range candidates {
		for _, metaKey := range []string{"provider_metadata", "proxy_metadata"} {
			if pm, _ := cand[metaKey].(map[string]any); pm != nil {
				if gw, _ := pm["gateway"].(map[string]any); gw != nil {
					if routing, _ := gw["routing"].(map[string]any); routing != nil {
						if fp, _ := routing["finalProvider"].(string); fp != "" {
							return "vercel/planner", fp
						}
					}
				}
			}
		}
	}
	if p, _ := obj["provider"].(string); p != "" {
		return "openrouter/direct", p
	}
	return "", ""
}

// ---- 会话粘性路由（缓存亲和）----
// 上游 prompt 缓存按「账号 + 前缀」分桶：多账号轮询会让同一对话每一轮换桶，缓存全冷。
// convAffinity 把「模型 + 对话前缀」粘到固定账号（滑动 TTL），同一对话始终命中同一账号的热缓存；
// 新对话照常走轮询策略负载均衡。账号失效 / 请求失败时解绑，下一轮自动重选。
const (
	convAffinityTTL     = 45 * time.Minute
	convAffinityMaxKeys = 8192
)

type convAffinityEntry struct {
	AccountID string
	Expires   time.Time
}

var (
	convAffinityMu sync.Mutex
	convAffinity   = map[string]convAffinityEntry{}
)

// stringContent 提取消息 content 的文本部分（string 或多段 parts 数组），用于派生稳定的对话指纹。
func stringContent(v any) string {
	switch c := v.(type) {
	case string:
		return c
	case []any:
		var b strings.Builder
		for _, p := range c {
			if pm, ok := p.(map[string]any); ok {
				if t, ok := pm["text"].(string); ok {
					b.WriteString(t)
				}
			}
		}
		return b.String()
	}
	return ""
}

// conversationKey 由「模型 + 首条消息内容前 512 字节」派生：
// 对话追加历史不改变首条消息 → 同一对话所有轮次得到相同 key。
func conversationKey(model string, params map[string]any) string {
	prefix := ""
	if msgs, ok := params["messages"].([]any); ok && len(msgs) > 0 {
		if m, ok := msgs[0].(map[string]any); ok {
			prefix = stringContent(m["content"])
		}
	}
	if len(prefix) > 512 {
		prefix = prefix[:512]
	}
	sum := sha256.Sum256([]byte(model + "\x00" + prefix))
	return fmt.Sprintf("%x", sum[:16])
}

// accountByIDActiveEligible：账号存在、active 且该模型未处于模型级冷却时返回它。
func accountByIDActiveEligible(id, model string) *Account {
	p := loadPool()
	poolMu.Lock()
	defer poolMu.Unlock()
	for _, a := range p.Accounts {
		if a.AccountID == id && a.Status == "active" {
			if until, cool := a.ModelCooldowns[model]; cool && time.Now().Before(until) {
				return nil
			}
			return a
		}
	}
	return nil
}

// sweepConvAffinityLocked 清理过期条目（调用方持有 convAffinityMu）。
func sweepConvAffinityLocked() {
	if len(convAffinity) <= convAffinityMaxKeys {
		return
	}
	now := time.Now()
	for k, e := range convAffinity {
		if now.After(e.Expires) {
			delete(convAffinity, k)
		}
	}
}

// pickAccountForConversation 粘性选号：亲和命中且账号健康 → 复用（滑动续期）；
// 否则按既有策略选号并记录亲和。
// 全程持有 convAffinityMu：同一对话的并发首拍只会选一次号，后续请求全部粘住同一账号
// （否则并发竞态会把同一对话劈到不同账号，缓存当场碎片化）。
func pickAccountForConversation(model string, params map[string]any) *Account {
	key := conversationKey(model, params)
	now := time.Now()

	convAffinityMu.Lock()
	defer convAffinityMu.Unlock()

	if e, ok := convAffinity[key]; ok && now.Before(e.Expires) {
		if acc := accountByIDActiveEligible(e.AccountID, model); acc != nil {
			e.Expires = now.Add(convAffinityTTL)
			convAffinity[key] = e
			return acc
		}
		delete(convAffinity, key) // 账号失效/冷却，解绑重选
	}
	sweepConvAffinityLocked()

	acc := pickAccountForModel(model)
	if acc != nil {
		convAffinity[key] = convAffinityEntry{AccountID: acc.AccountID, Expires: now.Add(convAffinityTTL)}
	}
	return acc
}

// evictConversationAffinity 账号故障时解除粘性，下一轮请求换号重试。
func evictConversationAffinity(model string, params map[string]any) {
	key := conversationKey(model, params)
	convAffinityMu.Lock()
	delete(convAffinity, key)
	convAffinityMu.Unlock()
}

// maybeAliasToClinePass 裸名模型被 zen 判为付费时，若 Cline Pass 侧存在同名模型
// （cline-pass/<id>），返回该别名；其余情况返回空串。
// 场景：客户端发裸名 deepseek-v4.1-flash（该 ID 在 opencode 目录里是付费模型，会被直接拒绝），
// 而订阅内 cline-pass/deepseek-v4.1-flash 可用 —— 回退改写而不是 400。
func maybeAliasToClinePass(id string) string {
	trimmed := strings.TrimSpace(id)
	if trimmed == "" || routeModel(trimmed) != "reject" {
		return ""
	}
	alias := "cline-pass/" + strings.TrimPrefix(trimmed, "opencode/")
	for _, m := range getAllModels() {
		if m.ID == alias && m.Status == "active" {
			return alias
		}
	}
	return ""
}

type chatRequest struct {
	Model               string          `json:"model"`
	Messages            json.RawMessage `json:"messages"`
	Stream              bool            `json:"stream,omitempty"`
	MaxTokens           int             `json:"max_tokens,omitempty"`
	MaxCompletionTokens int             `json:"max_completion_tokens,omitempty"`
	Tools               json.RawMessage `json:"tools,omitempty"`
	ToolChoice          json.RawMessage `json:"tool_choice,omitempty"`
	ReasoningEffort     string          `json:"reasoning_effort,omitempty"`
	ReasoningEffortAlt  string          `json:"reasoningEffort,omitempty"`
	Extra               map[string]any  `json:"-"`
}

func startProxy(host string, port int) error {
	p := loadPool()
	loadRequestLogs()
	activeCount := 0
	for _, a := range p.Accounts {
		if a.Status == "active" {
			// Try to pre-warm tokens
			if a.AccessToken == "" || time.Now().UnixMilli() >= a.ExpiresAt {
				if err := refreshAccountToken(a); err != nil {
					log.Printf("  Pre-warm failed for %s: %v", a.Email, err)
					continue
				}
			}
			activeCount++
		}
	}
	log.Printf("Loaded %d active accounts from pool", activeCount)

	// 启动时异步同步一次 Cline 官方推荐模型（不阻塞启动）
	startModelSync()

	// opencode zen：定时同步免费模型列表 + 压缩会话状态清理
	if getZenConfig().Enabled {
		startZenModelsRefresher()
	}
	startCompactCleanup()

	freePort(port)

	mux := http.NewServeMux()

	mux.HandleFunc("/v1/health", corsHandler(func(w http.ResponseWriter, r *http.Request) {
		info := map[string]any{
			"status":         "ok",
			"version":        appVersion,
			"activeAccounts": activeCount,
		}
		writeJSON(w, http.StatusOK, info)
	}))
	mux.HandleFunc("/health", corsHandler(func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{
			"status":         "ok",
			"version":        appVersion,
			"activeAccounts": activeCount,
		})
	}))

	// Admin API (frontend + REST)
	registerAdminRoutes(mux)

	apiKeyHandler := func(next http.HandlerFunc) http.HandlerFunc {
		return corsHandler(func(w http.ResponseWriter, r *http.Request) {
			// Allow requests without key if no keys configured
			p := loadPool()
			if len(p.Keys) == 0 {
				next(w, r)
				return
			}

			key := r.Header.Get("x-api-key")
			if key == "" {
				if b := r.Header.Get("Authorization"); len(b) > 7 && b[:7] == "Bearer " {
					key = b[7:]
				}
			}

			valid := false
			for _, k := range p.Keys {
				if k == key {
					valid = true
					break
				}
			}

			if !valid {
				writeJSON(w, http.StatusUnauthorized, map[string]any{
					"error": map[string]string{
						"message": "invalid API key. Generate one at /admin/ or set x-api-key header",
						"type":    "auth_error",
					},
				})
				return
			}
			next(w, r)
		})
	}

	modelsHandler := apiKeyHandler(func(w http.ResponseWriter, r *http.Request) {
		all := getAllModels()
		list := make([]map[string]any, len(all))
		for i, m := range all {
			ownedBy := "cline"
			if m.Source == "zen" || m.Provider == "opencode" {
				ownedBy = "opencode"
			}
			list[i] = map[string]any{
				"id":       m.ID,
				"object":   "model",
				"created":  time.Now().UnixMilli(),
				"owned_by": ownedBy,
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"object": "list", "data": list})
	})
	mux.HandleFunc("/v1/models", modelsHandler)
	mux.HandleFunc("/models", modelsHandler)

	chatHandler := apiKeyHandler(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		if activeCount == 0 && len(loadPool().Accounts) == 0 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": map[string]string{
					"message": "No accounts in pool. Run with --add-account or POST /admin/login to add accounts.",
					"type":    "auth_error",
				},
			})
			return
		}

		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "parse_error"},
			})
			return
		}

		var params map[string]any
		if err := json.Unmarshal(body, &params); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "parse_error"},
			})
			return
		}

		isStream, _ := params["stream"].(bool)
		toolCount := 0
		if tools, ok := params["tools"]; ok {
			if t, ok := tools.([]any); ok {
				toolCount = len(t)
			}
		}
		model, _ := params["model"].(string)
		log.Printf("  client: stream=%v tools=%d model=%s", isStream, toolCount, model)

		reqLog := RequestLog{StartedAt: time.Now(), Protocol: "openai", Model: model, Stream: isStream}

		// Override system prompt from override.md for OpenAI format
		if override := loadOverrideContent(); override != "" {
			if msgs, ok := params["messages"].([]any); ok {
				found := false
				for _, m := range msgs {
					if mm, ok := m.(map[string]any); ok {
						if mm["role"] == "system" {
							mm["content"] = override
							found = true
							break
						}
					}
				}
				if !found {
					params["messages"] = append([]any{map[string]any{"role": "system", "content": override}}, msgs...)
				}
			}
		}

		// 裸名付费 zen 模型回退到 Cline Pass 同名模型（如 deepseek-v4.1-flash → cline-pass/deepseek-v4.1-flash）
		if alt := maybeAliasToClinePass(model); alt != "" {
			log.Printf("  alias: %s -> %s (paid opencode, available via Cline Pass)", model, alt)
			params["model"] = alt
			model = alt
			reqLog.Model = alt
		}

		// 按 model 自动分流：zen 免费模型 / zen 付费拒绝 / 其余走 Cline 池
		switch routeModel(model) {
		case "reject":
			msg := fmt.Sprintf("model %q is a paid opencode model; only free models are proxied", model)
			finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, msg)
			writeJSON(w, http.StatusBadRequest, map[string]any{
				"error": map[string]string{"message": msg, "type": "invalid_request_error"},
			})
			return
		case "zen":
			reqLog.Upstream = upstreamOpenCode
			zm, _ := resolveZenInfo(model)
			out := maybeCompact(params, zm, requestSessionID(params, r.Header))
			if out.changed {
				log.Printf("  chat %s", out.note)
			}
			resp, err := callZenAPI(params, isStream)
			if err != nil {
				log.Printf("  api error: %v", err)
				finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, err.Error())
				writeJSON(w, http.StatusBadGateway, map[string]any{
					"error": map[string]string{"message": err.Error(), "type": "api_error"},
				})
				return
			}
			defer resp.Body.Close()
			if isStream {
				handleStreamResponse(w, resp, nil, &reqLog)
			} else {
				handleNonStreamResponse(w, resp, nil, &reqLog)
			}
			return
		}

		if activeCount == 0 && len(loadPool().Accounts) == 0 {
			writeJSON(w, http.StatusUnauthorized, map[string]any{
				"error": map[string]string{
					"message": "No accounts in pool. Run with --add-account or POST /admin/login to add accounts.",
					"type":    "auth_error",
				},
			})
			return
		}

		resp, acc, err := callClineAPI(params, isStream)
		if effectiveModel, ok := params["model"].(string); ok && effectiveModel != "" {
			reqLog.Model = effectiveModel
		}
		if err != nil {
			log.Printf("  api error: %v", err)
			finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, err.Error())
			writeJSON(w, clineErrorHTTPStatus(err), map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "api_error"},
			})
			return
		}
		reqLog.Upstream = upstreamCline
		defer resp.Body.Close()
		if acc != nil {
			reqLog.AccountID = acc.AccountID
			reqLog.AccountEmail = acc.Email
		}

		if isStream {
			handleStreamResponse(w, resp, acc, &reqLog)
		} else {
			handleNonStreamResponse(w, resp, acc, &reqLog)
		}
	})
	mux.HandleFunc("/v1/chat/completions", chatHandler)
	mux.HandleFunc("/chat/completions", chatHandler)

	// Anthropic Messages API support
	anthropicHandler := apiKeyHandler(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		handleAnthropicMessages(w, r)
	})
	mux.HandleFunc("/v1/messages", anthropicHandler)
	mux.HandleFunc("/messages", anthropicHandler)

	// OpenAI Responses API support（所有上游：zen 免费模型 + Cline 账号池）
	responsesHandler := apiKeyHandler(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
			return
		}
		handleResponses(w, r)
	})
	mux.HandleFunc("/v1/responses", responsesHandler)
	mux.HandleFunc("/responses", responsesHandler)

	if host == "" {
		host = "127.0.0.1"
	}
	addr := fmt.Sprintf("%s:%d", host, port)
	listenHost = host
	listenPort = port
	serverMux = mux
	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}
	serverMu.Lock()
	currentServer = server
	serverMu.Unlock()

	// 启动后台冷却恢复巡检
	startCooldownRecovery()

	fmt.Println("")
	fmt.Println(strings.Repeat("=", 58))
	fmt.Printf("  Cline Go Proxy %s - No CLI Required\n", appVersion)
	fmt.Println(strings.Repeat("=", 58))
	fmt.Printf("  http://%s\n", addr)
	fmt.Printf("  http://%s/v1\n", addr)
	if !isLoopbackHost(host) {
		for _, ip := range detectLocalIPs() {
			fmt.Printf("  http://%s:%d (LAN)\n", ip, port)
		}
		fmt.Println("  !!! 监听非本机地址，管理后台无鉴权，请确认网络环境安全")
	}
	fmt.Println("  API Key: any value")
	fmt.Printf("  Model:   %s\n", getDefaultModel())
	fmt.Printf("  Accounts: %d total, %d active\n", len(loadPool().Accounts), activeCount)
	if zc := getZenConfig(); zc.Enabled {
		fmt.Printf("  OpenCode: enabled (%s free models)\n", strings.TrimRight(zc.BaseURL, "/"))
	} else {
		fmt.Println("  OpenCode: disabled")
	}
	fmt.Println(strings.Repeat("=", 58))

	return server.ListenAndServe()
}

func corsHandler(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization, x-api-key, anthropic-version, anthropic-beta")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		h(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

// ---- 上游请求体捕获（错误诊断用）----
// 部分上游错误以 HTTP 200 + SSE error 事件返回，事后无处查看请求体。
// 每条上游请求记录脱敏摘要（shape + content 类型 + base64 打码），流内错误时打印最后一条。
var (
	lastBodiesMu sync.Mutex
	lastBodies   []string
)

// redactJSON 递归打码长 base64 / data URL，保留结构可读。
func redactJSON(v any) any {
	switch t := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, vv := range t {
			out[k] = redactJSON(vv)
		}
		return out
	case []any:
		out := make([]any, 0, len(t))
		for _, vv := range t {
			out = append(out, redactJSON(vv))
		}
		return out
	case string:
		if strings.HasPrefix(t, "data:") {
			return fmt.Sprintf("<data-url %dB>", len(t))
		}
		if len(t) > 1024 {
			// 仅当形似 base64（无空白的紧凑字符集）才标 b64，长普通文本标 str
			compact := true
			for _, r := range t {
				if r == ' ' || r == '\n' || r == '\t' || r == '\r' {
					compact = false
					break
				}
			}
			if compact {
				return fmt.Sprintf("<b64 %dB>", len(t))
			}
			return fmt.Sprintf("<str %dB> %.60s...", len(t), t[:min(60, len(t))])
		}
		return t
	}
	return v
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func recordUpstreamBody(body map[string]any) {
	red := redactJSON(body["messages"])
	b, err := json.Marshal(red)
	if err != nil {
		return
	}
	const maxLen = 4000
	s := string(b)
	if len(s) > maxLen {
		s = s[:maxLen] + "...(truncated)"
	}
	lastBodiesMu.Lock()
	lastBodies = append(lastBodies, s)
	if len(lastBodies) > 10 {
		lastBodies = lastBodies[len(lastBodies)-10:]
	}
	lastBodiesMu.Unlock()
}

func dumpLastBody() {
	lastBodiesMu.Lock()
	defer lastBodiesMu.Unlock()
	if len(lastBodies) == 0 {
		return
	}
	log.Printf("  last upstream body (redacted): %s", lastBodies[len(lastBodies)-1])
}

// normalizeMessageParts 把漏进 OpenAI 请求体的 Anthropic 形态 content 块翻译成合法 parts
// （new-api 等转换层会原样漏出 image/tool_result/tool_use/thinking/document 等块，
// 上游对未知 part 类型一律 400 Invalid input）。
// 返回 (新消息, 提取出的 tool_use 调用, 是否有改动)。
func normalizeMessageParts(msg map[string]any) (map[string]any, []any, bool) {
	arr, ok := msg["content"].([]any)
	if !ok {
		return msg, nil, false
	}
	role, _ := msg["role"].(string)
	changed := false
	out := make([]any, 0, len(arr))
	var toolCalls []any
	var toolResults []any // user 消息里漏出的 tool_result 块（由 cleanMessages 编排处理）
	for _, p := range arr {
		pm, ok := p.(map[string]any)
		if !ok {
			out = append(out, p)
			continue
		}
		switch pm["type"] {
		case "text":
			out = append(out, pm)
		case "image_url":
			// 字符串简写形态：{"type":"image_url","image_url":"data:..."} → 对象形态
			if s, ok := pm["image_url"].(string); ok {
				out = append(out, map[string]any{"type": "image_url", "image_url": map[string]any{"url": s}})
				changed = true
				continue
			}
			if iu, ok := pm["image_url"].(map[string]any); ok {
				// detail 显式 null（部分客户端如此发送）会被上游 400 拒绝 —— 剥除；
				// detail 为合法字符串值时保留
				if d, present := iu["detail"]; present && d == nil {
					ciu := make(map[string]any, len(iu))
					for k, v := range iu {
						ciu[k] = v
					}
					delete(ciu, "detail")
					cpm := make(map[string]any, len(pm))
					for k, v := range pm {
						cpm[k] = v
					}
					cpm["image_url"] = ciu
					out = append(out, cpm)
					changed = true
					continue
				}
			}
			out = append(out, pm)
		case "image":
			if src, ok := pm["source"].(map[string]any); ok {
				if u := anthropicImageURL(src); u != "" {
					out = append(out, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}})
					changed = true
					continue
				}
			}
			changed = true // 无法解析的图片块 → 丢弃
		case "tool_result":
			if role == "user" {
				changed = true
				toolResults = append(toolResults, pm)
			} else {
				out = append(out, pm)
			}
		case "tool_use":
			// 漏进 content 的 tool_use 块 → 提升为消息级 tool_calls
			changed = true
			argsStr := "{}"
			if input, ok := pm["input"]; ok && input != nil {
				if s, ok := input.(string); ok {
					argsStr = s
				} else if b, err := json.Marshal(input); err == nil {
					argsStr = string(b)
				}
			}
			toolCalls = append(toolCalls, map[string]any{
				"id":       pm["id"],
				"type":     "function",
				"function": map[string]any{"name": pm["name"], "arguments": argsStr},
			})
		case "thinking", "redacted_thinking":
			changed = true // 丢弃
		case "document":
			changed = true
			mt := ""
			if src, ok := pm["source"].(map[string]any); ok {
				if v, _ := src["media_type"].(string); v != "" {
					mt = v
				}
			}
			out = append(out, map[string]any{"type": "text", "text": "[document attached: " + mt + "]"})
		default:
			// 未知块类型 → 文本占位，保结构合法
			changed = true
			out = append(out, map[string]any{"type": "text", "text": fmt.Sprintf("[unsupported block: %v]", pm["type"])})
		}
	}
	if !changed {
		return msg, nil, false
	}
	cp := make(map[string]any, len(msg)+1)
	for k, v := range msg {
		cp[k] = v
	}
	switch {
	case len(out) == 0 && (len(toolCalls) > 0 || len(toolResults) > 0):
		cp["content"] = ""
	case len(out) == 0:
		cp["content"] = " "
	default:
		cp["content"] = out
	}
	if len(toolCalls) > 0 {
		cp["tool_calls"] = toolCalls
	}
	if len(toolResults) > 0 {
		cp["__leaked_tool_results"] = toolResults // 内部标记，cleanMessages 消费后移除
	}
	return cp, toolCalls, true
}

// toolResultToMessages 把漏出的 tool_result 块转成 role:tool 消息（配对前一条 assistant 的 tool_calls）
// 或（无配对时）文本占位 parts。
func toolResultToMessages(blocks []any, callIDSet map[string]bool) ([]any, []any) {
	var toolMsgs, fallbackParts []any
	for _, b := range blocks {
		bm, _ := b.(map[string]any)
		if bm == nil {
			continue
		}
		id, _ := bm["tool_use_id"].(string)
		if id != "" && callIDSet[id] {
			content := ""
			images := []any{}
			switch c := bm["content"].(type) {
			case string:
				content = c
			case []any:
				texts := []string{}
				for _, pb := range c {
					if pm, ok := pb.(map[string]any); ok {
						switch pm["type"] {
						case "text":
							if t, ok := pm["text"].(string); ok {
								texts = append(texts, t)
							}
						case "image":
							if src, ok := pm["source"].(map[string]any); ok {
								if u := anthropicImageURL(src); u != "" {
									images = append(images, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}})
								}
							}
						}
					}
				}
				content = strings.Join(texts, "\n")
			}
			toolMsgs = append(toolMsgs, map[string]any{
				"role": "tool", "tool_call_id": id, "content": content,
			})
			fallbackParts = append(fallbackParts, images...) // 图片不能进 tool 消息，并入后续 user content
			continue
		}
		fallbackParts = append(fallbackParts, toolResultBlockToParts(bm)...)
	}
	return toolMsgs, fallbackParts
}

// toolResultBlockToParts 把无配对的 tool_result 块降级为 text + image_url parts。
func toolResultBlockToParts(tr map[string]any) []any {
	parts := []any{}
	switch c := tr["content"].(type) {
	case string:
		parts = append(parts, map[string]any{"type": "text", "text": "[tool_result] " + c})
	case []any:
		texts := []string{}
		for _, b := range c {
			bm, ok := b.(map[string]any)
			if !ok {
				continue
			}
			switch bm["type"] {
			case "text":
				if t, ok := bm["text"].(string); ok {
					texts = append(texts, t)
				}
			case "image":
				if src, ok := bm["source"].(map[string]any); ok {
					if u := anthropicImageURL(src); u != "" {
						parts = append(parts, map[string]any{"type": "image_url", "image_url": map[string]any{"url": u}})
					}
				}
			}
		}
		label := "[tool_result]"
		if len(texts) > 0 {
			label += " " + strings.Join(texts, "\n")
		}
		parts = append([]any{map[string]any{"type": "text", "text": label}}, parts...)
	default:
		parts = append(parts, map[string]any{"type": "text", "text": "[tool_result]"})
	}
	return parts
}

func cleanMessages(messages []any) []any {
	cleaned := make([]any, 0, len(messages))
	// 上一条 assistant 消息的 tool_call id 集合（用于把漏出的 tool_result 配对成 role:tool 消息）
	prevCallIDs := map[string]bool{}

	callIDSet := func(msg map[string]any) map[string]bool {
		set := map[string]bool{}
		if tcs, ok := msg["tool_calls"].([]any); ok {
			for _, tc := range tcs {
				if tcm, ok := tc.(map[string]any); ok {
					if id, ok := tcm["id"].(string); ok && id != "" {
						set[id] = true
					}
				}
			}
		}
		return set
	}

	for _, m := range messages {
		msg, ok := m.(map[string]any)
		if !ok {
			cleaned = append(cleaned, m)
			continue
		}
		// 上游校验比 OpenAI 严（实测边界）：
		//   - content 空数组 []            → 400 Invalid input（任何角色）
		//   - user / tool 角色 content ""  → 400 Invalid input
		//   - assistant 角色 content ""    → 合法
		//   - user / tool 角色 content " " → 合法
		//   - 任何未知 part 类型（Anthropic 块泄漏等）→ 400 Invalid input
		role, _ := msg["role"].(string)

		if _, isArr := msg["content"].([]any); isArr {
			nm, _, changed := normalizeMessageParts(msg)
			// 消费内部标记：漏出的 tool_result 块
			var leaked []any
			if lv, ok := nm["__leaked_tool_results"].([]any); ok {
				leaked = lv
				delete(nm, "__leaked_tool_results")
			}
			if len(leaked) > 0 {
				toolMsgs, extraParts := toolResultToMessages(leaked, prevCallIDs)
				cleaned = append(cleaned, toolMsgs...)
				// 剩余 parts（含 tool_result 里的图片）并入本条消息 content
				if len(extraParts) > 0 {
					cur, _ := nm["content"].([]any)
					nm["content"] = append(cur, extraParts...)
				}
				changed = true
			}
			// content 可能被清成空数组（全部是 tool_result 时）
			if arr, _ := nm["content"].([]any); len(arr) == 0 {
				if nm["role"] == "assistant" {
					nm["content"] = ""
				} else {
					nm["content"] = " "
				}
			}
			if changed {
				prevCallIDs = callIDSet(nm)
				cleaned = append(cleaned, nm)
				continue
			}
			prevCallIDs = callIDSet(msg)
			cleaned = append(cleaned, msg)
			continue
		}

		if s, ok := msg["content"].(string); ok && s == "" && (role == "user" || role == "tool") {
			cp := make(map[string]any, len(msg))
			for k, v := range msg {
				cp[k] = v
			}
			cp["content"] = " "
			cleaned = append(cleaned, cp)
			prevCallIDs = map[string]bool{}
			continue
		}
		prevCallIDs = callIDSet(msg)
		cleaned = append(cleaned, msg)
	}
	return cleaned
}

// logMessageShapes 上游 400 时打印每条消息的 content 形状（role + 类型 + parts 构成），
// 用于定位"Invalid input, param=messages.N.content"类校验错误的真实来源。
func logMessageShapes(body map[string]any) {
	msgs, _ := body["messages"].([]any)
	var b strings.Builder
	for i, m := range msgs {
		mm, ok := m.(map[string]any)
		if !ok {
			fmt.Fprintf(&b, " [%d:non-obj]", i)
			continue
		}
		role, _ := mm["role"].(string)
		switch c := mm["content"].(type) {
		case string:
			fmt.Fprintf(&b, " [%d:%s str=%d]", i, role, len(c))
		case []any:
			types := make([]string, 0, len(c))
			for _, p := range c {
				if pm, ok := p.(map[string]any); ok {
					if t, _ := pm["type"].(string); t != "" {
						types = append(types, t)
					} else {
						types = append(types, "?")
					}
				}
			}
			fmt.Fprintf(&b, " [%d:%s parts=%v]", i, role, types)
		case nil:
			fmt.Fprintf(&b, " [%d:%s nil]", i, role)
		default:
			fmt.Fprintf(&b, " [%d:%s %T]", i, role, c)
		}
	}
	log.Printf("  upstream 400 body shapes:%s", b.String())
}

// sessionSeq 保证并发突发下 session_id 全局唯一（毫秒时间戳在高并发下会碰撞，
// 上游若按 task 归并状态，碰撞就是串扰源）。
var sessionSeq atomic.Int64

func buildUpstreamBody(params map[string]any, stream bool) map[string]any {
	sessionID := fmt.Sprintf("sess_%d_%d", time.Now().UnixMilli(), sessionSeq.Add(1))

	maxTokens := defaultMaxTokens
	if mt, ok := params["max_tokens"].(float64); ok {
		maxTokens = int(mt)
	} else if mt, ok := params["max_completion_tokens"].(float64); ok {
		maxTokens = int(mt)
	}

	model := getDefaultModel()
	if m, ok := params["model"].(string); ok && m != "" {
		model = m
	}

	body := map[string]any{
		"model":            model,
		"max_tokens":       maxTokens,
		"session_id":       sessionID,
		"reasoning_effort": defaultReasoningEffort,
	}

	if msgsRaw, ok := params["messages"]; ok {
		if msgsArr, ok := msgsRaw.([]any); ok {
			body["messages"] = cleanMessages(msgsArr)
		} else {
			body["messages"] = msgsRaw
		}
	}

	if stream {
		body["stream"] = true
	}

	if re, ok := params["reasoning_effort"].(string); ok && re != "" {
		body["reasoning_effort"] = re
	} else if re, ok := params["reasoningEffort"].(string); ok && re != "" {
		body["reasoning_effort"] = re
	}

	for _, key := range passThroughKeys {
		if val, ok := params[key]; ok {
			body[key] = val
		}
	}

	// Provider pin：客户端显式传入 providerOptions / provider 时以其为准（保住 __probe__ 探测通路），
	// 否则按模型映射表注入，把指定模型固定到官方上游。
	if _, hasGateway := body["providerOptions"].(map[string]any); !hasGateway {
		if _, hasDirect := body["provider"].(map[string]any); !hasDirect {
			if pin, ok := lookupProviderPin(model); ok {
				switch pin.field {
				case "gateway":
					body["providerOptions"] = map[string]any{
						"gateway": map[string]any{"only": []string{pin.slug}},
					}
				case "direct":
					body["provider"] = map[string]any{"only": []string{pin.slug}}
				}
				log.Printf("  provider pin: model=%s field=%s slug=%s", model, pin.field, pin.slug)
			}
		}
	}

	return body
}

func clineHeaders(token, sessionID string) http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+token)
	h.Set("Content-Type", "application/json")
	h.Set("X-Task-ID", sessionID)

	cfg := getProxyConfig()
	for k, v := range cfg.Headers {
		h.Set(k, v)
	}

	return h
}

type clineAPIError struct {
	statusCode int
	message    string
}

func (e *clineAPIError) Error() string {
	return fmt.Sprintf("API %d: %s", e.statusCode, e.message)
}

type clineAccountUnavailableError struct {
	err error
}

func (e *clineAccountUnavailableError) Error() string {
	return e.err.Error()
}

func (e *clineAccountUnavailableError) Unwrap() error {
	return e.err
}

type freeModelUnavailableError struct {
	message string
}

func (e *freeModelUnavailableError) Error() string {
	return e.message
}

func clineErrorHTTPStatus(err error) int {
	if _, ok := err.(*freeModelUnavailableError); ok {
		return http.StatusTooManyRequests
	}
	return http.StatusInternalServerError
}

func callClineAPI(params map[string]any, stream bool) (*http.Response, *Account, error) {
	model, _ := params["model"].(string)
	if model == "free" {
		return callFreeClineAPI(params, stream)
	}

	// 会话粘性：同一对话固定同一账号，保住该账号上的上游 prompt 缓存
	acc := pickAccountForConversation(model, params)
	if acc == nil {
		return nil, nil, fmt.Errorf("no active accounts available. Use --login or admin API to add accounts")
	}
	resp, picked, err := callClineAPIWithAccount(acc, params, stream)
	if err == nil {
		return resp, picked, err
	}
	var unavailable *clineAccountUnavailableError
	if errors.As(err, &unavailable) {
		evictConversationAffinity(model, params)
	}
	// 稳定性：官方端点（zai / deepseek / meta）思维链会烧尽小 max_tokens 预算，
	// 上游以 500 "empty response content" 收场（流式请求同样在开流前以 500 状态返回，
	// 可安全重试）。单次重试，max_tokens 提到 4 倍（封顶 8192），仍失败则原样上抛。
	var apiErr *clineAPIError
	if errors.As(err, &apiErr) && apiErr.statusCode == 500 &&
		strings.Contains(apiErr.message, "empty response content") && bumpMaxTokensForRetry(params) {
		log.Printf("  retry with bumped max_tokens=%v (empty response content)", params["max_tokens"])
		return callClineAPIWithAccount(acc, params, stream)
	}
	return resp, picked, err
}

// bumpMaxTokensForRetry 空响应重试的预算提升：×4 封顶 8192。
// 客户端未显式设置 max_tokens 时不重试（默认 128000 已足够，空响应另有原因）。
func bumpMaxTokensForRetry(params map[string]any) bool {
	cur := 0.0
	switch v := params["max_tokens"].(type) {
	case float64:
		cur = v
	case int:
		cur = float64(v)
	default:
		return false
	}
	if cur <= 0 || cur >= 8192 {
		return false
	}
	next := cur * 4
	if next > 8192 {
		next = 8192
	}
	if next <= cur {
		return false
	}
	params["max_tokens"] = next
	return true
}

func callFreeClineAPI(params map[string]any, stream bool) (*http.Response, *Account, error) {
	for _, model := range freeModelChain {
		params["model"] = model
		for {
			acc := pickAccountForModelStrict(model)
			if acc == nil {
				break
			}

			resp, usedAcc, err := callClineAPIWithAccount(acc, params, stream)
			if err == nil {
				return resp, usedAcc, nil
			}
			var accountErr *clineAccountUnavailableError
			if errors.As(err, &accountErr) {
				continue
			}
			apiErr, ok := err.(*clineAPIError)
			if !ok || apiErr.statusCode != http.StatusTooManyRequests {
				return nil, usedAcc, err
			}
		}
	}
	return nil, nil, &freeModelUnavailableError{message: "no eligible accounts available for free models"}
}

func callClineAPIWithAccount(acc *Account, params map[string]any, stream bool) (*http.Response, *Account, error) {
	token, err := ensureAccountToken(acc)
	if err != nil {
		// Try other accounts
		return nil, acc, &clineAccountUnavailableError{err: fmt.Errorf("account %s token failed: %w", acc.Email, err)}
	}

	body := buildUpstreamBody(params, stream)
	sessionID, _ := body["session_id"].(string)

	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, acc, fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequest("POST", clineAPIBase+"/chat/completions", bytes.NewReader(bodyJSON))
	if err != nil {
		return nil, acc, fmt.Errorf("create request: %w", err)
	}
	req.Header = clineHeaders(token, sessionID)

	toolCount := 0
	if tools, ok := params["tools"]; ok {
		if t, ok := tools.([]any); ok {
			toolCount = len(t)
		}
	}
	log.Printf("  upstream: account=%s stream=%v tools=%d msgs=%d max_tokens=%v effort=%v",
		truncateEmail(acc.Email), stream, toolCount, getMsgCount(params), body["max_tokens"], body["reasoning_effort"])
	recordUpstreamBody(body)

	resp, err := httpClient.Do(req)
	if err != nil {
		acc.Status = "cooldown"
		acc.CooldownUntil = time.Now().Add(5 * time.Minute)
		savePool()
		return nil, acc, &clineAccountUnavailableError{err: fmt.Errorf("upstream request: %w", err)}
	}

	if resp.StatusCode == 401 {
		resp.Body.Close()
		// Refresh token and retry
		if err := refreshAccountToken(acc); err == nil {
			token = acc.AccessToken
			req.Header = clineHeaders(token, sessionID)
			req.Body = io.NopCloser(bytes.NewReader(bodyJSON))
			resp, err = httpClient.Do(req)
			if err != nil {
				acc.Status = "cooldown"
				acc.CooldownUntil = time.Now().Add(5 * time.Minute)
				savePool()
				return nil, acc, &clineAccountUnavailableError{err: fmt.Errorf("upstream retry: %w", err)}
			}
			if resp.StatusCode == 401 {
				resp.Body.Close()
				acc.Status = "expired"
				savePool()
				return nil, acc, &clineAccountUnavailableError{err: fmt.Errorf("account %s token expired permanently", acc.Email)}
			}
		} else {
			acc.Status = "expired"
			savePool()
			return nil, acc, &clineAccountUnavailableError{err: fmt.Errorf("account %s refresh failed: %w", acc.Email, err)}
		}
	}

	if resp.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		bodyStr := string(bodyBytes)
		if resp.StatusCode == 400 {
			logMessageShapes(body)
		}
		// 429：模型级冷却 —— 只暂停该模型，账号保持可用，其他模型继续转发
		if resp.StatusCode == 429 {
			model, _ := body["model"].(string)
			until := parseCooldownUntil(bodyStr)
			if model != "" {
				setModelCooldown(acc, model, until)
			} else {
				acc.Status = "cooldown"
				acc.CooldownUntil = until
				savePool()
			}
		}
		return nil, acc, &clineAPIError{statusCode: resp.StatusCode, message: truncate(bodyStr, 500)}
	}

	acc.LastUsed = time.Now()
	acc.UsageCount++
	savePool()
	return resp, acc, nil
}

type accountTestResult struct {
	AccountID    string `json:"accountId"`
	Email        string `json:"email"`
	OK           bool   `json:"ok"`
	DurationMs   int64  `json:"durationMs"`
	InputTokens  int64  `json:"inputTokens"`
	OutputTokens int64  `json:"outputTokens"`
	Error        string `json:"error,omitempty"`
}

// parseCooldownUntil 从 429 响应体中解析 "Try again in 1h 1m" 格式的等待时长，
// 返回预计恢复时间；解析失败则回退到 1 小时后。
var cooldownRe = regexp.MustCompile(`(?i)try\s+again\s+in\s+(\d+)\s*h?(?:\s*(\d+))?\s*m?`)

func parseCooldownUntil(body string) time.Time {
	matches := cooldownRe.FindStringSubmatch(body)
	if len(matches) >= 2 {
		hours, _ := strconv.Atoi(matches[1])
		minutes := 0
		if len(matches) >= 3 && matches[2] != "" {
			minutes, _ = strconv.Atoi(matches[2])
		}
		if hours > 0 || minutes > 0 {
			return time.Now().Add(time.Duration(hours)*time.Hour + time.Duration(minutes)*time.Minute)
		}
	}
	// 解析失败，回退 1 小时
	return time.Now().Add(1 * time.Hour)
}

// startCooldownRecovery 启动后台 goroutine，每 30 秒检查一次 cooldown 账号，
// 对 CooldownUntil 已过期的账号执行探活，成功则自动激活。
func startCooldownRecovery() {
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			p := loadPool()
			poolMu.Lock()
			var toRecover []*Account
			for _, acc := range p.Accounts {
				if acc.Status != "cooldown" {
					continue
				}
				// 有恢复时间且已过期 → 探活
				// 无恢复时间（旧数据）→ 也尝试探活
				if acc.CooldownUntil.IsZero() || time.Now().After(acc.CooldownUntil) {
					toRecover = append(toRecover, acc)
				}
			}
			poolMu.Unlock()

			for _, acc := range toRecover {
				log.Printf("cooldown recovery: testing %s", acc.Email)
				result := testAccount(acc)
				if result.OK {
					log.Printf("cooldown recovery: %s reactivated", acc.Email)
				} else {
					log.Printf("cooldown recovery: %s still unavailable: %s", acc.Email, result.Error)
				}
			}
		}
	}()
}

// testAccount sends a minimal "hi" request through a specific account to verify
// it can complete an upstream call. It does not update aggregate token counters
// or request logs; it is a diagnostic-only probe.
func testAccount(acc *Account) accountTestResult {
	result := accountTestResult{AccountID: acc.AccountID, Email: acc.Email}
	started := time.Now()

	params := map[string]any{
		"model":      getDefaultModel(),
		"max_tokens": 16,
		"stream":     false,
		"messages": []any{
			map[string]any{"role": "user", "content": "hi"},
		},
	}

	resp, _, err := callClineAPIWithAccount(acc, params, false)
	if err != nil {
		result.DurationMs = time.Since(started).Milliseconds()
		result.Error = truncate(err.Error(), 200)
		return result
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		result.DurationMs = time.Since(started).Milliseconds()
		result.Error = "read response: " + truncate(err.Error(), 200)
		return result
	}

	var obj map[string]any
	if err := json.Unmarshal(raw, &obj); err != nil {
		result.DurationMs = time.Since(started).Milliseconds()
		result.Error = "decode response: " + truncate(err.Error(), 200)
		return result
	}
	if data, ok := obj["data"]; ok {
		if d, ok := data.(map[string]any); ok {
			obj = d
		}
	}
	obj = normalizeOpenAIResponse(obj)
	usage := parseTokenUsage(obj["usage"])

	result.OK = true
	result.DurationMs = time.Since(started).Milliseconds()
	if usage.Valid {
		result.InputTokens = usage.Prompt
		result.OutputTokens = usage.Completion
	}
	// If the account was in cooldown/expired but the test succeeded, restore it.
	if acc.Status != "active" {
		poolMu.Lock()
		acc.Status = "active"
		poolMu.Unlock()
		savePool()
	}
	return result
}

type tokenUsage struct {
	Prompt     int64
	Completion int64
	Total      int64
	Cached     int64
	Valid      bool
}

func parseTokenUsage(value any) tokenUsage {
	usage, ok := value.(map[string]any)
	if !ok {
		return tokenUsage{}
	}
	read := func(keys ...string) int64 {
		for _, key := range keys {
			if value, ok := usage[key].(float64); ok && value >= 0 {
				return int64(value)
			}
		}
		return 0
	}
	readNested := func(parent string, keys ...string) int64 {
		details, ok := usage[parent].(map[string]any)
		if !ok {
			return 0
		}
		for _, key := range keys {
			if value, ok := details[key].(float64); ok && value >= 0 {
				return int64(value)
			}
		}
		return 0
	}
	prompt := read("prompt_tokens", "input_tokens")
	completion := read("completion_tokens", "output_tokens")
	cached := int64(0)
	if nested := readNested("prompt_tokens_details", "cached_tokens"); nested > 0 {
		cached = nested
	} else if nested := readNested("input_tokens_details", "cached_tokens"); nested > 0 {
		cached = nested
	} else if v := read("cache_read_input_tokens") + read("cache_creation_input_tokens"); v > 0 {
		cached = v
	} else {
		cached = read("prompt_cache_hit_tokens", "prompt_cache_creation_tokens", "cached_tokens")
	}
	total := read("total_tokens")
	if total == 0 {
		total = prompt + completion
	}
	_, hasUsage := usage["prompt_tokens"]
	if !hasUsage {
		_, hasUsage = usage["input_tokens"]
		if !hasUsage {
			if _, hasUsage = usage["completion_tokens"]; !hasUsage {
				if _, hasUsage = usage["output_tokens"]; !hasUsage {
					_, hasUsage = usage["total_tokens"]
				}
			}
		}
	}
	if !hasUsage {
		_, hasUsage = usage["cache_read_input_tokens"]
		if !hasUsage {
			_, hasUsage = usage["cache_creation_input_tokens"]
			if !hasUsage {
				_, hasUsage = usage["prompt_tokens_details"]
				if !hasUsage {
					_, hasUsage = usage["input_tokens_details"]
				}
			}
		}
	}
	return tokenUsage{Prompt: prompt, Completion: completion, Total: total, Cached: cached, Valid: hasUsage}
}

func mergeTokenUsage(current, next tokenUsage) tokenUsage {
	if !next.Valid {
		return current
	}
	if next.Prompt != 0 {
		current.Prompt = next.Prompt
	}
	if next.Completion != 0 {
		current.Completion = next.Completion
	}
	if next.Total != 0 {
		current.Total = next.Total
	}
	if next.Cached != 0 {
		current.Cached = next.Cached
	}
	current.Valid = current.Valid || next.Valid
	if current.Total == 0 && (current.Prompt != 0 || current.Completion != 0) {
		current.Total = current.Prompt + current.Completion
	}
	return current
}

func recordTokenUsage(acc *Account, model string, usage tokenUsage) {
	if acc == nil || !usage.Valid {
		return
	}
	// 先判断是否免费模型（getAllModels 会拿 poolMu，必须在持有锁之前计算）
	isFree := model != "" && isFreeModelID(model)
	poolMu.Lock()
	acc.PromptTokens += usage.Prompt
	acc.CompletionTokens += usage.Completion
	acc.TotalTokens += usage.Total
	acc.CachedTokens += usage.Cached
	// 按模型细分统计（仅记录 free 模型）
	if isFree {
		if acc.ModelStats == nil {
			acc.ModelStats = make(map[string]*ModelStat)
		}
		st := acc.ModelStats[model]
		if st == nil {
			st = &ModelStat{ModelID: model, Cost: "free"}
			acc.ModelStats[model] = st
		}
		st.UsageCount++
		st.PromptTokens += usage.Prompt
		st.CompletionTokens += usage.Completion
		st.TotalTokens += usage.Total
		st.CachedTokens += usage.Cached
	}
	poolMu.Unlock()
	savePool()
}

// isFreeModelID 判断模型是否为 free 计费（用于按模型统计和模型级冷却）。
func isFreeModelID(model string) bool {
	for _, m := range getAllModels() {
		if m.ID == model {
			return m.Cost == "free"
		}
	}
	// 未知模型：按 ID 后缀/前缀启发式判断
	return strings.HasSuffix(model, ":free") || strings.Contains(model, "/free/")
}

// modelCooldownActive 判断某账号下该模型是否处于模型级冷却中。
func modelCooldownActive(acc *Account, model string) bool {
	if acc == nil || model == "" {
		return false
	}
	poolMu.Lock()
	defer poolMu.Unlock()
	until, ok := acc.ModelCooldowns[model]
	if !ok {
		return false
	}
	if time.Now().After(until) {
		delete(acc.ModelCooldowns, model)
		savePool()
		return false
	}
	return true
}

// setModelCooldown 记录模型级冷却（429 时调用）：只暂停该模型，账号保持可用。
// fallback 为解析失败时的恢复时长（默认 1 小时）。
func setModelCooldown(acc *Account, model string, until time.Time) {
	if acc == nil || model == "" {
		return
	}
	poolMu.Lock()
	if acc.ModelCooldowns == nil {
		acc.ModelCooldowns = make(map[string]time.Time)
	}
	acc.ModelCooldowns[model] = until
	poolMu.Unlock()
	savePool()
	log.Printf("model cooldown: account=%s model=%s until=%s", truncateEmail(acc.Email), model, until.Format("15:04:05"))
}

func truncateEmail(email string) string {
	if len(email) <= 12 {
		return email
	}
	parts := splitEmail(email)
	if len(parts) == 2 && len(parts[0]) > 3 {
		return parts[0][:3] + "***@" + parts[1]
	}
	if len(email) > 12 {
		return email[:8] + "..."
	}
	return email
}

func splitEmail(email string) []string {
	for i := 0; i < len(email); i++ {
		if email[i] == '@' {
			return []string{email[:i], email[i+1:]}
		}
	}
	return []string{email}
}

func getMsgCount(params map[string]any) int {
	if msgs, ok := params["messages"].([]any); ok {
		return len(msgs)
	}
	return 0
}

func handleStreamResponse(w http.ResponseWriter, upstream *http.Response, acc *Account, reqLog *RequestLog) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)

	flusher, ok := w.(http.Flusher)
	if !ok {
		log.Printf("  streaming not supported for client")
		return
	}

	reader := bufio.NewReader(upstream.Body)
	var latestUsage tokenUsage
	var firstOutputAt time.Time
	lastRouteLog := ""
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				if line != "" {
					w.Write([]byte(line + "\n"))
				}
			}
			break
		}

		line = strings.TrimRight(line, "\r\n")

		if strings.HasPrefix(line, "data:") {
			payload := strings.TrimSpace(line[5:])
			if payload == "" || payload == "[DONE]" {
				w.Write([]byte(line + "\n\n"))
				flusher.Flush()
				continue
			}

			// Try to normalize the response
			var obj map[string]any
			if err := json.Unmarshal([]byte(payload), &obj); err == nil {
				// 上游部分错误以 HTTP 200 + SSE error 事件返回（如 stream_initialization_failed），
				// 不打日志就完全不可见；同时 dump 脱敏请求体定位非法 content 形态
				if e, ok := obj["error"]; ok {
					log.Printf("  upstream stream error: %s", truncate(fmt.Sprint(e), 200))
					dumpLastBody()
				}
				// 在 normalize 剥离 metadata 之前抓实际路由结果
				if pl, prov := routingInfo(obj); prov != "" && pl+"/"+prov != lastRouteLog {
					lastRouteLog = pl + "/" + prov
					log.Printf("  upstream routing: pipeline=%s provider=%s", pl, prov)
				}
				// Some Cline responses wrap in {data: {...}}
				if data, ok := obj["data"]; ok {
					if d, ok := data.(map[string]any); ok {
						if _, hasChoices := d["choices"]; hasChoices {
							obj = d
						}
						if _, hasID := d["id"]; hasID {
							obj = d
						}
					}
				}
				normalized := normalizeOpenAIResponse(obj)
				if usage := parseTokenUsage(normalized["usage"]); usage.Valid {
					latestUsage = mergeTokenUsage(latestUsage, usage)
				}
				if firstOutputAt.IsZero() && hasFirstOutput(normalized) {
					firstOutputAt = time.Now()
				}
				if normBytes, err := json.Marshal(normalized); err == nil {
					w.Write([]byte("data: " + string(normBytes) + "\n\n"))
					flusher.Flush()
					continue
				}
			}
		}

		w.Write([]byte(line + "\n"))
		flusher.Flush()
	}
	recordTokenUsage(acc, reqLog.Model, latestUsage)
	finalizeRequestLog(reqLog, latestUsage, firstOutputAt, reqLog.StartedAt, true, "")
}

func hasFirstOutput(obj map[string]any) bool {
	choices, ok := getNested(obj, "choices").([]any)
	if !ok || len(choices) == 0 {
		return false
	}
	choice, _ := choices[0].(map[string]any)
	if choice == nil {
		return false
	}
	if delta, ok := choice["delta"].(map[string]any); ok {
		if c, _ := delta["content"].(string); c != "" {
			return true
		}
		if tc, ok := delta["tool_calls"].([]any); ok && len(tc) > 0 {
			return true
		}
	}
	if msg, ok := choice["message"].(map[string]any); ok {
		if c, _ := msg["content"].(string); c != "" {
			return true
		}
		if tc, ok := msg["tool_calls"].([]any); ok && len(tc) > 0 {
			return true
		}
	}
	return false
}

func handleNonStreamResponse(w http.ResponseWriter, upstream *http.Response, acc *Account, reqLog *RequestLog) {
	var raw map[string]any
	if err := json.NewDecoder(upstream.Body).Decode(&raw); err != nil {
		finalizeRequestLog(reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, "decode response: "+err.Error())
		writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "parse_error"},
		})
		return
	}

	// Some Cline responses wrap in {data: {...}}
	out := raw
	if data, ok := raw["data"]; ok {
		if d, ok := data.(map[string]any); ok {
			out = d
		}
	}

	// 在 normalize 剥离 metadata 之前抓实际路由结果（wrapper 内外都查）
	for _, candidate := range []map[string]any{raw, out} {
		if pl, prov := routingInfo(candidate); prov != "" {
			log.Printf("  upstream routing: pipeline=%s provider=%s", pl, prov)
			break
		}
	}

	out = normalizeOpenAIResponse(out)
	usage := parseTokenUsage(out["usage"])
	recordTokenUsage(acc, reqLog.Model, usage)
	finalizeRequestLog(reqLog, usage, time.Time{}, reqLog.StartedAt, true, "")

	if msg, ok := getNested(out, "choices", 0, "message").(map[string]any); ok {
		tc, _ := msg["tool_calls"].([]any)
		content, _ := msg["content"].(string)
		log.Printf("  nonstream finish=%v tool_calls=%d content_len=%d",
			getNested(out, "choices", 0, "finish_reason"),
			len(tc), len(content))
	}

	writeJSON(w, http.StatusOK, out)
}

// Anthropic Messages API support
type anthropicMsg struct {
	Role    string `json:"role"`
	Content any    `json:"content"`
}

type toolAccumulator struct {
	index   int
	id      string
	name    string
	args    string
	emitted bool
}

type anthropicReq struct {
	Model       string          `json:"model"`
	MaxTokens   int             `json:"max_tokens"`
	Messages    []anthropicMsg  `json:"messages"`
	System      json.RawMessage `json:"system,omitempty"`
	Stream      bool            `json:"stream,omitempty"`
	Temperature float64         `json:"temperature,omitempty"`
	TopP        float64         `json:"top_p,omitempty"`
	TopK        int             `json:"top_k,omitempty"`
	Stop        json.RawMessage `json:"stop_sequences,omitempty"`
	Tools       json.RawMessage `json:"tools,omitempty"`
	ToolChoice  json.RawMessage `json:"tool_choice,omitempty"`
	Metadata    json.RawMessage `json:"metadata,omitempty"`
	Extra       map[string]any  `json:"-"`
}

func loadOverrideContent() string {
	data, err := os.ReadFile("override.md")
	if err != nil {
		log.Printf("  override.md not found: %v", err)
		return ""
	}
	content := strings.TrimSpace(string(data))
	if content != "" {
		log.Printf("  using override.md as system prompt (%d bytes)", len(content))
	} else {
		log.Printf("  override.md is empty")
	}
	return content
}

// anthropicImageURL 把 Anthropic image source 转成 OpenAI image_url 的 url 字段：
// base64 源 → data:<media_type>;base64,<data>；url 源 → 原样返回；不认识的形态返回空串。
func anthropicImageURL(src map[string]any) string {
	switch src["type"] {
	case "base64":
		mt, _ := src["media_type"].(string)
		data, _ := src["data"].(string)
		if data == "" {
			return ""
		}
		if mt == "" {
			mt = "image/png"
		}
		return "data:" + mt + ";base64," + data
	case "url":
		if u, ok := src["url"].(string); ok && u != "" {
			return u
		}
	}
	return ""
}

func extractStringContent(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	// Try string first
	var s string
	if err := json.Unmarshal(raw, &s); err == nil {
		return s
	}
	// Try array of content blocks
	var blocks []map[string]any
	if err := json.Unmarshal(raw, &blocks); err == nil {
		parts := []string{}
		for _, b := range blocks {
			if b["type"] == "text" {
				if t, ok := b["text"].(string); ok {
					parts = append(parts, t)
				}
			}
		}
		return strings.Join(parts, "\n")
	}
	return ""
}

func anthropicToolsToOpenAI(tools []any) []any {
	out := make([]any, 0, len(tools))
	for _, t := range tools {
		if tMap, ok := t.(map[string]any); ok {
			// Already in OpenAI format
			if tMap["type"] == "function" {
				out = append(out, t)
				continue
			}
			// Convert Anthropic format to OpenAI
			oai := map[string]any{
				"type": "function",
				"function": map[string]any{
					"name":        tMap["name"],
					"description": tMap["description"],
					"parameters":  tMap["input_schema"],
				},
			}
			out = append(out, oai)
		}
	}
	return out
}

func anthropicToOpenAI(req anthropicReq) map[string]any {
	openAI := map[string]any{
		"model":      req.Model,
		"max_tokens": req.MaxTokens,
		"stream":     req.Stream,
		"messages":   []any{},
	}
	if req.Temperature != 0 {
		openAI["temperature"] = req.Temperature
	}
	if req.TopP != 0 {
		openAI["top_p"] = req.TopP
	}
	// Convert Anthropic tools to OpenAI format
	if req.Tools != nil {
		var toolsArr []any
		if err := json.Unmarshal(req.Tools, &toolsArr); err == nil {
			openAI["tools"] = anthropicToolsToOpenAI(toolsArr)
		}
	}
	if req.ToolChoice != nil {
		openAI["tool_choice"] = req.ToolChoice
	}

	msgs := []any{}

	// System prompt: use override.md if it exists, otherwise use Anthropic's system field
	sysContent := loadOverrideContent()
	if sysContent == "" && req.System != nil {
		sysContent = extractStringContent(req.System)
	}
	if sysContent != "" {
		log.Printf("  system prompt: %d bytes (from override.md)", len(sysContent))
		msgs = append(msgs, map[string]any{"role": "system", "content": sysContent})
	}

	for _, m := range req.Messages {
		switch c := m.Content.(type) {
		case string:
			msgs = append(msgs, map[string]any{"role": m.Role, "content": c})
		case []any:
			textParts := []string{}
			var toolCalls []any
			var toolResult *map[string]any
			imageParts := []any{}
			toolResultImages := []any{}

			for _, block := range c {
				if b, ok := block.(map[string]any); ok {
					switch b["type"] {
					case "text":
						if t, ok := b["text"].(string); ok {
							textParts = append(textParts, t)
						}
					case "image":
						// Anthropic image block → OpenAI image_url part（不再丢弃）
						// source 支持 base64 与 url 两种形态
						if src, ok := b["source"].(map[string]any); ok {
							if u := anthropicImageURL(src); u != "" {
								imageParts = append(imageParts, map[string]any{
									"type":      "image_url",
									"image_url": map[string]any{"url": u},
								})
							}
						}
					case "tool_use":
						argsStr := "{}"
						if input, ok := b["input"]; ok && input != nil {
							if s, ok := input.(string); ok {
								argsStr = s
							} else if bts, err := json.Marshal(input); err == nil {
								argsStr = string(bts)
							}
						}
						tc := map[string]any{
							"id":   b["id"],
							"type": "function",
							"function": map[string]any{
								"name":      b["name"],
								"arguments": argsStr,
							},
						}
						toolCalls = append(toolCalls, tc)
					case "tool_result":
						// content 可能是 string 或 blocks 数组 —— agent 客户端（Claude Code 等）的
						// 图片通常包在 tool_result 里：文本进 tool 消息，图片提取到后续合成
						// user 消息送上游，否则模型根本看不到图、只会反复调工具去"读图"
						trContent := b["content"]
						if blocks, ok := trContent.([]any); ok {
							texts := []string{}
							for _, blk := range blocks {
								if bm, ok := blk.(map[string]any); ok {
									switch bm["type"] {
									case "text":
										if t, ok := bm["text"].(string); ok {
											texts = append(texts, t)
										}
									case "image":
										if src, ok := bm["source"].(map[string]any); ok {
											if u := anthropicImageURL(src); u != "" {
												toolResultImages = append(toolResultImages, map[string]any{
													"type":      "image_url",
													"image_url": map[string]any{"url": u},
												})
											}
										}
									}
								}
							}
							trContent = strings.Join(texts, "\n")
						}
						tr := map[string]any{
							"role":         "tool",
							"content":      trContent,
							"tool_call_id": b["tool_use_id"],
						}
						toolResult = &tr
					}
				}
			}

			if m.Role == "assistant" && len(toolCalls) > 0 {
				msg := map[string]any{
					"role":       "assistant",
					"content":    strings.Join(textParts, "\n"),
					"tool_calls": toolCalls,
				}
				msgs = append(msgs, msg)
			} else if m.Role == "user" && toolResult != nil {
				msgs = append(msgs, *toolResult)
				// tool_result 里的图片以合成 user 消息跟进（OpenAI tool 消息只收文本），
				// 模型因此能直接看到图，不再靠工具调用绕路取图
				if len(toolResultImages) > 0 {
					parts := append([]any{map[string]any{"type": "text", "text": "[images returned by the tool call above]"}}, toolResultImages...)
					msgs = append(msgs, map[string]any{"role": "user", "content": parts})
					log.Printf("  anthropic: %d image(s) extracted from tool_result", len(toolResultImages))
				}
			} else if len(imageParts) > 0 {
				// 带图消息：content 用 parts 数组（text part + image_url parts），保序：先文后图
				parts := make([]any, 0, len(textParts)+len(imageParts))
				for _, t := range textParts {
					parts = append(parts, map[string]any{"type": "text", "text": t})
				}
				parts = append(parts, imageParts...)
				msgs = append(msgs, map[string]any{"role": m.Role, "content": parts})
			} else {
				content := strings.Join(textParts, "\n")
				msgs = append(msgs, map[string]any{"role": m.Role, "content": content})
			}
		}
	}

	openAI["messages"] = msgs
	return openAI
}

func openAIToAnthropic(openAI map[string]any) map[string]any {
	out := map[string]any{
		"id":    "msg_" + fmt.Sprintf("%x", time.Now().UnixMilli()),
		"type":  "message",
		"role":  "assistant",
		"model": getNested(openAI, "model"),
	}

	choices := getNested(openAI, "choices")
	if choices == nil {
		out["content"] = []any{map[string]any{"type": "text", "text": ""}}
		out["stop_reason"] = "end_turn"
		out["usage"] = map[string]any{"input_tokens": 0, "output_tokens": 0}
		return out
	}

	choice0 := getNested(openAI, "choices", 0).(map[string]any)
	msg, _ := choice0["message"].(map[string]any)
	if msg == nil {
		msg, _ = choice0["delta"].(map[string]any)
	}

	text := ""
	if msg != nil {
		if c, ok := msg["content"].(string); ok {
			text = sanitizeContent(c)
		}
	}

	contentBlocks := []any{map[string]any{"type": "text", "text": text}}

	// Convert tool_calls to Anthropic tool_use blocks
	if msg != nil {
		if tc, ok := msg["tool_calls"].([]any); ok && len(tc) > 0 {
			contentBlocks = []any{} // Clear text-only, proper response has both
			if text != "" {
				contentBlocks = append(contentBlocks, map[string]any{"type": "text", "text": text})
			}
			for _, tcItem := range tc {
				if tcMap, ok := tcItem.(map[string]any); ok {
					funcData, _ := tcMap["function"].(map[string]any)
					input := funcData["arguments"]
					// OpenAI arguments is a JSON string; Anthropic expects an object
					if argsStr, ok := input.(string); ok {
						var argsObj any
						if json.Unmarshal([]byte(argsStr), &argsObj) == nil {
							input = argsObj
						}
					}
					block := map[string]any{
						"type":  "tool_use",
						"id":    tcMap["id"],
						"name":  funcData["name"],
						"input": input,
					}
					contentBlocks = append(contentBlocks, block)
				}
			}
		}
	}

	out["content"] = contentBlocks

	switch getNested(openAI, "choices", 0, "finish_reason") {
	case "stop":
		out["stop_reason"] = "end_turn"
	case "length":
		out["stop_reason"] = "max_tokens"
	case "tool_calls":
		out["stop_reason"] = "tool_use"
	default:
		out["stop_reason"] = "end_turn"
	}

	usage := map[string]any{}
	if u := getNested(openAI, "usage"); u != nil {
		if um, ok := u.(map[string]any); ok {
			usage["input_tokens"] = um["prompt_tokens"]
			usage["output_tokens"] = um["completion_tokens"]
			// 缓存字段映射：Anthropic 客户端（Claude Code 等）靠 cache_read_input_tokens
			// 展示缓存命中率，缺了会永远显示 0% 缓存
			cached := int64(0)
			readUsage := func(keys ...string) int64 {
				for _, key := range keys {
					if v, ok := um[key].(float64); ok && v >= 0 {
						return int64(v)
					}
				}
				return 0
			}
			if details, ok := um["prompt_tokens_details"].(map[string]any); ok {
				if v, ok := details["cached_tokens"].(float64); ok && v >= 0 {
					cached = int64(v)
				}
			}
			if cached == 0 {
				cached = readUsage("cache_read_input_tokens", "prompt_cache_hit_tokens")
			}
			if cached > 0 {
				usage["cache_read_input_tokens"] = cached
			}
			if cc := readUsage("cache_creation_input_tokens"); cc > 0 {
				usage["cache_creation_input_tokens"] = cc
			}
		}
	}
	out["usage"] = usage

	return out
}

func handleAnthropicMessages(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "parse_error"},
		})
		return
	}

	var req anthropicReq
	if err := json.Unmarshal(body, &req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "parse_error"},
		})
		return
	}

	if len(req.Messages) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]string{"message": "messages is required", "type": "parse_error"},
		})
		return
	}

	if req.MaxTokens == 0 {
		req.MaxTokens = defaultMaxTokens
	}

	openAIReq := anthropicToOpenAI(req)

	log.Printf("  anthropic: model=%s stream=%v msgs=%d", req.Model, req.Stream, len(req.Messages))

	reqLog := RequestLog{StartedAt: time.Now(), Protocol: "anthropic", Model: req.Model, Stream: req.Stream}

	// 裸名付费 zen 模型回退到 Cline Pass 同名模型（与 chat 端点一致）
	if alt := maybeAliasToClinePass(req.Model); alt != "" {
		log.Printf("  anthropic alias: %s -> %s (paid opencode, available via Cline Pass)", req.Model, alt)
		req.Model = alt
		openAIReq["model"] = alt
		reqLog.Model = alt
	}

	// 按 model 自动分流（与 chat 端点一致）：zen 免费/付费拒绝/Cline 池
	switch routeModel(req.Model) {
	case "reject":
		msg := fmt.Sprintf("model %q is a paid opencode model; only free models are proxied", req.Model)
		finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, msg)
		writeJSON(w, http.StatusBadRequest, map[string]any{
			"error": map[string]string{"message": msg, "type": "invalid_request_error"},
		})
		return
	case "zen":
		reqLog.Upstream = upstreamOpenCode
		zm, _ := resolveZenInfo(req.Model)
		out := maybeCompact(openAIReq, zm, requestSessionID(map[string]any{"session_id": r.Header.Get("x-opencode-session")}, nil))
		if out.changed {
			log.Printf("  anthropic %s", out.note)
		}
		resp, err := callZenAPI(openAIReq, req.Stream)
		if err != nil {
			log.Printf("  anthropic api error: %v", err)
			finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, err.Error())
			writeJSON(w, http.StatusBadGateway, map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "api_error"},
			})
			return
		}
		defer resp.Body.Close()
		if req.Stream {
			handleAnthropicStream(w, resp, nil, &reqLog)
		} else {
			var raw map[string]any
			if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
				finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, "decode response: "+err.Error())
				writeJSON(w, http.StatusInternalServerError, map[string]any{
					"error": map[string]string{"message": err.Error(), "type": "parse_error"},
				})
				return
			}
			out2 := normalizeOpenAIResponse(unwrapDataEnvelope(raw))
			usage := parseTokenUsage(out2["usage"])
			finalizeRequestLog(&reqLog, usage, time.Time{}, reqLog.StartedAt, true, "")
			anthropicResp := openAIToAnthropic(out2)
			if tc, ok := getNested(out2, "choices", 0, "message", "tool_calls").([]any); ok && len(tc) > 0 {
				anthropicResp["content"] = []any{}
				anthropicResp["stop_reason"] = "tool_use"
			}
			writeJSON(w, http.StatusOK, anthropicResp)
		}
		return
	}

	activeCount := 0
	p := loadPool()
	for _, a := range p.Accounts {
		if a.Status == "active" {
			activeCount++
		}
	}

	if activeCount == 0 && len(p.Accounts) == 0 {
		writeJSON(w, http.StatusUnauthorized, map[string]any{
			"error": map[string]string{
				"message": "No accounts in pool",
				"type":    "auth_error",
			},
		})
		return
	}

	resp, acc, err := callClineAPI(openAIReq, req.Stream)
	if effectiveModel, ok := openAIReq["model"].(string); ok && effectiveModel != "" {
		reqLog.Model = effectiveModel
	}
	if err != nil {
		log.Printf("  anthropic api error: %v", err)
		finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, err.Error())
		writeJSON(w, clineErrorHTTPStatus(err), map[string]any{
			"error": map[string]string{"message": err.Error(), "type": "api_error"},
		})
		return
	}
	reqLog.Upstream = upstreamCline
	defer resp.Body.Close()
	if acc != nil {
		reqLog.AccountID = acc.AccountID
		reqLog.AccountEmail = acc.Email
	}

	if req.Stream {
		handleAnthropicStream(w, resp, acc, &reqLog)
	} else {
		var raw map[string]any
		if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
			finalizeRequestLog(&reqLog, tokenUsage{}, time.Time{}, reqLog.StartedAt, false, "decode response: "+err.Error())
			writeJSON(w, http.StatusInternalServerError, map[string]any{
				"error": map[string]string{"message": err.Error(), "type": "parse_error"},
			})
			return
		}
		out := raw
		if data, ok := raw["data"]; ok {
			if d, ok := data.(map[string]any); ok {
				out = d
			}
		}
		out = normalizeOpenAIResponse(out)
		usage := parseTokenUsage(out["usage"])
		recordTokenUsage(acc, reqLog.Model, usage)
		finalizeRequestLog(&reqLog, usage, time.Time{}, reqLog.StartedAt, true, "")
		anthropicResp := openAIToAnthropic(out)

		if tc, ok := getNested(out, "choices", 0, "message", "tool_calls").([]any); ok && len(tc) > 0 {
			anthropicResp["content"] = []any{}
			anthropicResp["stop_reason"] = "tool_use"
		}

		writeJSON(w, http.StatusOK, anthropicResp)
	}
}

func handleAnthropicStream(w http.ResponseWriter, upstream *http.Response, acc *Account, reqLog *RequestLog) {
	log.Printf("  anthropic stream: starting real-time forward")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(http.StatusOK)
	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}

	emit := func(event string, data any) {
		d, _ := json.Marshal(data)
		w.Write([]byte(fmt.Sprintf("event: %s\n", event)))
		w.Write([]byte(fmt.Sprintf("data: %s\n\n", string(d))))
		flusher.Flush()
	}

	msgID := "msg_" + fmt.Sprintf("%x", time.Now().UnixMilli())
	stopReason := "end_turn"
	emit("message_start", map[string]any{
		"type": "message_start",
		"message": map[string]any{
			"id":          msgID,
			"type":        "message",
			"role":        "assistant",
			"content":     []any{},
			"model":       "",
			"stop_reason": nil,
		},
	})

	textIndex := new(int)
	*textIndex = -1
	hasText := false
	pendingTools := map[int]*toolAccumulator{}

	emitToolBlock := func(acc *toolAccumulator) {
		acc.emitted = true
		var argsObj any
		json.Unmarshal([]byte(acc.args), &argsObj)
		if argsObj == nil {
			argsObj = map[string]any{}
		}
		emit("content_block_start", map[string]any{
			"type":  "content_block_start",
			"index": acc.index,
			"content_block": map[string]any{
				"type":  "tool_use",
				"id":    acc.id,
				"name":  acc.name,
				"input": argsObj,
			},
		})
		emit("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": acc.index,
		})
	}

	reader := bufio.NewReader(upstream.Body)
	var latestUsage tokenUsage
	var firstOutputAt time.Time

	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "data:") {
			continue
		}
		payload := strings.TrimSpace(line[5:])
		if payload == "" || payload == "[DONE]" {
			continue
		}

		var obj map[string]any
		if err := json.Unmarshal([]byte(payload), &obj); err != nil {
			continue
		}
		if data, ok := obj["data"]; ok {
			if d, ok := data.(map[string]any); ok {
				obj = d
			}
		}
		if usage := parseTokenUsage(obj["usage"]); usage.Valid {
			latestUsage = mergeTokenUsage(latestUsage, usage)
		}
		if firstOutputAt.IsZero() && hasFirstOutput(obj) {
			firstOutputAt = time.Now()
		}

		// Detect upstream SSE error
		if errPayload, ok := obj["error"]; ok {
			errBody, _ := json.Marshal(errPayload)
			log.Printf("  upstream SSE error: %s", string(errBody))
			emit("error", map[string]any{"type": "error", "error": errPayload})
			break
		}

		choices, _ := getNested(obj, "choices").([]any)
		if len(choices) == 0 {
			continue
		}
		choice, _ := choices[0].(map[string]any)
		if choice == nil {
			continue
		}

		delta, ok := choice["delta"].(map[string]any)
		if !ok {
			delta = choice
		}

		// Text content delta
		if c, ok := delta["content"].(string); ok && c != "" {
			if !hasText {
				hasText = true
				*textIndex++
				emit("content_block_start", map[string]any{
					"type":  "content_block_start",
					"index": *textIndex,
					"content_block": map[string]any{
						"type": "text",
						"text": "",
					},
				})
			}
			emit("content_block_delta", map[string]any{
				"type":  "content_block_delta",
				"index": *textIndex,
				"delta": map[string]any{
					"type": "text_delta",
					"text": sanitizeContent(c),
				},
			})
		}

		// Tool calls - accumulate and emit when complete
		if tcRaw, ok := delta["tool_calls"].([]any); ok {
			for _, tc := range tcRaw {
				tcMap, _ := tc.(map[string]any)
				if tcMap == nil {
					continue
				}
				idx := 0
				if i, ok := tcMap["index"].(float64); ok {
					idx = int(i)
				}
				acc, exists := pendingTools[idx]
				if !exists {
					acc = &toolAccumulator{index: idx}
					pendingTools[idx] = acc
				}
				if id, ok := tcMap["id"].(string); ok && id != "" {
					acc.id = id
				}
				if fn, ok := tcMap["function"].(map[string]any); ok {
					if name, ok := fn["name"].(string); ok && name != "" {
						acc.name = name
					}
					if args, ok := fn["arguments"].(string); ok && args != "" {
						acc.args += args
					}
				}
				if acc.id != "" && acc.name != "" && acc.args != "" && !acc.emitted {
					emitToolBlock(acc)
				}
			}
		}

		// Finish reason
		if fr, ok := choice["finish_reason"].(string); ok && fr != "" {
			switch fr {
			case "length":
				stopReason = "max_tokens"
			case "tool_calls":
				stopReason = "tool_use"
			}
		}
	}

	// Stop text block if active
	if hasText {
		emit("content_block_stop", map[string]any{
			"type":  "content_block_stop",
			"index": *textIndex,
		})
	}

	// Emit any remaining un-emitted tool blocks
	for _, acc := range pendingTools {
		if !acc.emitted {
			emitToolBlock(acc)
		}
	}

	streamUsage := map[string]any{
		"input_tokens":  latestUsage.Prompt,
		"output_tokens": latestUsage.Completion,
	}
	// 缓存字段：Anthropic 客户端靠 cache_read_input_tokens 展示缓存命中率
	if latestUsage.Cached > 0 {
		streamUsage["cache_read_input_tokens"] = latestUsage.Cached
	}
	emit("message_delta", map[string]any{
		"type": "message_delta",
		"delta": map[string]any{
			"stop_reason":   stopReason,
			"stop_sequence": nil,
		},
		"usage": streamUsage,
	})
	recordTokenUsage(acc, reqLog.Model, latestUsage)
	finalizeRequestLog(reqLog, latestUsage, firstOutputAt, reqLog.StartedAt, true, "")

	emit("message_stop", map[string]any{"type": "message_stop"})
	log.Printf("  anthropic stream done: hasText=%v tools=%d reason=%s", hasText, len(pendingTools), stopReason)
}

func normalizeOpenAIResponse(obj map[string]any) map[string]any {
	out := make(map[string]any)
	for k, v := range obj {
		if k == "provider_metadata" || k == "proxy_metadata" {
			continue
		}
		out[k] = v
	}

	if choices, ok := out["choices"].([]any); ok {
		normalized := make([]any, 0, len(choices))
		for _, ch := range choices {
			if c, ok := ch.(map[string]any); ok {
				nc := make(map[string]any)
				for k, v := range c {
					if k == "provider_metadata" || k == "proxy_metadata" {
						continue
					}
					nc[k] = v
				}
				if msg, ok := nc["message"].(map[string]any); ok {
					nc["message"] = normalizeMessage(msg)
				}
				if delta, ok := nc["delta"].(map[string]any); ok {
					nd := make(map[string]any)
					for k, v := range delta {
						if k == "provider_metadata" || k == "proxy_metadata" {
							continue
						}
						nd[k] = v
					}
					if tc, ok := nd["tool_calls"].([]any); ok && len(tc) > 0 {
						if nd["content"] == nil {
							nd["content"] = ""
						}
					}
					nc["delta"] = nd
				}
				normalized = append(normalized, nc)
			} else {
				normalized = append(normalized, ch)
			}
		}
		out["choices"] = normalized
	}

	return out
}

func sanitizeContent(s string) string {
	return s
}

func normalizeMessage(msg map[string]any) map[string]any {
	out := make(map[string]any)
	for k, v := range msg {
		if k == "provider_metadata" || k == "proxy_metadata" {
			continue
		}
		out[k] = v
	}
	if tc, ok := out["tool_calls"].([]any); ok && len(tc) > 0 {
		if out["content"] == nil {
			out["content"] = ""
		}
	}
	if c, ok := out["content"].(string); ok {
		out["content"] = sanitizeContent(c)
	}
	return out
}

func getNested(obj map[string]any, keys ...any) any {
	current := any(obj)
	for _, key := range keys {
		switch k := key.(type) {
		case string:
			if m, ok := current.(map[string]any); ok {
				current = m[k]
			} else {
				return nil
			}
		case int:
			if arr, ok := current.([]any); ok && k < len(arr) {
				current = arr[k]
			} else {
				return nil
			}
		default:
			return nil
		}
	}
	return current
}

func freePort(port int) {
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		return // port is free
	}
	conn.Close()

	// Try to kill the process using the port
	cmd := execCommand("powershell", "-Command",
		fmt.Sprintf(`$p=Get-NetTCPConnection -LocalPort %d -ErrorAction SilentlyContinue; if($p){Stop-Process -Id $p.OwningProcess -Force}`, port))
	_ = cmd.Run()
	time.Sleep(500 * time.Millisecond)
}
