package main

// usage.go 迁移 Cline 官方的 usage limit（app.cline.bot/dashboard/subscription 同源数据）。
//
// 上游接口（实测 2026-09-20，Bearer workos:<accessToken> 鉴权，clineHeaders 客户端头）：
//
//	GET /api/v1/users/me/plan/usage-limits
//	  {"data":{"limits":[
//	    {"type":"five_hour","percentUsed":1,"resetsAt":"2026-09-20T06:39:06Z"},
//	    {"type":"weekly","percentUsed":0,"resetsAt":"2026-09-27T01:39:06Z"},
//	    {"type":"monthly","percentUsed":0,"resetsAt":"2026-10-20T01:39:06Z"}]},"success":true}
//
//	GET /api/v1/users/me/plan
//	  {"data":{"plan":{"displayName":"Cline Pass (Monthly)","interval":"Monthly",...}}}
//
// 网关侧聚合为 GET /admin/api/usage（后台鉴权），逐账号返回限额百分比与重置时间；
// 60s 内存缓存——后台面板轮询不会打爆上游。

import (
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"
)

const usageCacheTTL = 60 * time.Second

type usageLimit struct {
	Type        string `json:"type"`
	PercentUsed int    `json:"percentUsed"`
	ResetsAt    string `json:"resetsAt"`
}

type accountUsage struct {
	AccountID string       `json:"accountId"`
	Email     string       `json:"email"`
	Plan      string       `json:"plan,omitempty"`
	Limits    []usageLimit `json:"limits"`
	Error     string       `json:"error,omitempty"`
	FetchedAt int64        `json:"fetchedAt"`
}

var (
	usageCacheMu sync.Mutex
	usageCache   = map[string]accountUsage{} // accountID -> 最近一次结果
	usageCacheAt time.Time
)

// httpGetJSONAuthed 带 Cline 客户端头的 GET，返回响应 body 字节。
func httpGetJSONAuthed(rawURL, token string) ([]byte, error) {
	req, err := http.NewRequest("GET", rawURL, nil)
	if err != nil {
		return nil, err
	}
	h := clineHeaders(token, "sess_admin_usage")
	for k, vs := range h {
		for _, v := range vs {
			req.Header.Add(k, v)
		}
	}
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 300))
		return nil, &clineAPIError{statusCode: resp.StatusCode, message: truncate(string(b), 200)}
	}
	return io.ReadAll(io.LimitReader(resp.Body, 1<<20))
}

// fetchAccountUsage 拉取单账号的官方限额与套餐名。
func fetchAccountUsage(acc *Account) accountUsage {
	au := accountUsage{AccountID: acc.AccountID, Email: acc.Email, FetchedAt: time.Now().Unix()}

	token, err := ensureAccountToken(acc)
	if err != nil {
		au.Error = "token: " + err.Error()
		return au
	}

	body, err := httpGetJSONAuthed(clineAPIBase+"/users/me/plan/usage-limits", token)
	if err != nil {
		au.Error = err.Error()
		return au
	}
	var limitsResp struct {
		Data struct {
			Limits []usageLimit `json:"limits"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &limitsResp); err != nil {
		au.Error = "decode usage-limits: " + err.Error()
		return au
	}
	au.Limits = limitsResp.Data.Limits

	// 套餐名（失败不致命）
	if body2, err := httpGetJSONAuthed(clineAPIBase+"/users/me/plan", token); err == nil {
		var planResp struct {
			Data struct {
				Plan struct {
					DisplayName string `json:"displayName"`
				} `json:"plan"`
			} `json:"data"`
		}
		if json.Unmarshal(body2, &planResp) == nil && planResp.Data.Plan.DisplayName != "" {
			au.Plan = planResp.Data.Plan.DisplayName
		}
	}
	return au
}

// handleAdminUsage GET /admin/api/usage：全账号限额聚合（60s 缓存）。
func handleAdminUsage(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		writeAPI(w, http.StatusMethodNotAllowed, apiResponse{Error: tAPI(r, "method_not_allowed")})
		return
	}
	p := loadPool()

	usageCacheMu.Lock()
	cacheFresh := time.Since(usageCacheAt) < usageCacheTTL
	usageCacheMu.Unlock()

	out := make([]accountUsage, 0, len(p.Accounts))
	for _, a := range p.Accounts {
		if cacheFresh {
			usageCacheMu.Lock()
			c, ok := usageCache[a.AccountID]
			usageCacheMu.Unlock()
			if ok {
				out = append(out, c)
				continue
			}
		}
		au := fetchAccountUsage(a)
		usageCacheMu.Lock()
		usageCache[a.AccountID] = au
		usageCacheMu.Unlock()
		out = append(out, au)
	}

	usageCacheMu.Lock()
	usageCacheAt = time.Now()
	usageCacheMu.Unlock()

	writeAPI(w, http.StatusOK, apiResponse{Success: true, Data: map[string]any{"accounts": out}})
}
