// Package main: upstream.go isolates the actual calls to the 无痕去水印 upstream
// API. This is the business layer — implement realUpstream against your provider
// and wire it in selectUpstream (channel.go). Serve is always live; the mock
// exists only so the offline `verify` subcommand runs without credentials or
// cost, and serve can never reach it.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
	wuhen "wuhen-sdk"
)

// PollResult is everything one poll of an upstream task tells us.
//
// Note the two distinct failure channels, and keep them apart:
//
//   - Poll returning a non-nil error means *the poll itself* failed (network,
//     auth, 5xx). The task's fate is still unknown, so the scheduler may retry.
//   - Poll returning Err set (with a FAILED status) means *the task* reached a
//     terminal failure upstream. Retrying can never help.
//
// Reporting a terminal task failure as a Go error is the most common mistake in
// a channel: it puts a dead task into a retry loop.
type PollResult struct {
	RawStatus string // the provider's own status string, forwarded verbatim
	Payload   *runtime.TaskPayload
	Progress  int32                 // 0-100, or -1 when the provider reports no progress
	Err       *runtime.ChannelError // set only for a terminal upstream failure
}

// Upstream is the minimal surface the channel needs from the provider. Keep the
// dispatcher's opaque input JSON as the Create argument; translate it to the
// provider's wire format (JSON, multipart/form-data, ...) inside your impl.
type Upstream interface {
	// Create submits a generation task and returns the provider task id.
	// SubmitInput carries exactly four fields: Input (opaque JSON params),
	// ModelID (target model), Account (credential/fingerprint plus the
	// account-bound proxy — use it on every upstream request), and
	// IdempotencyKey (pass through as the provider-side client request id
	// when supported, so a retried submit dedupes to the same upstream task).
	//
	// Hard contract: return as soon as the provider's own task id is bound and
	// NEVER wait for generation to finish here, even when the provider offers a
	// blocking wait mode — progress and results belong to Poll. The platform
	// bounds submit with CHANNEL_SUBMIT_TIMEOUT (default 16m): provider-side
	// task creation may take minutes, waiting for completion may not. The id
	// must be the provider's task id, handed back verbatim to Poll/Cancel.
	Create(ctx context.Context, input *channelv1.TaskInput, submit *runtime.SubmitInput) (providerTaskID string, err error)
	// Poll reads the current state of a task: one probe per call, return within
	// seconds. The platform bounds it with CHANNEL_CALL_TIMEOUT (default 30s)
	// and calls it on every poll tick; never wait for completion inside Poll.
	Poll(ctx context.Context, providerTaskID string, account *channelv1.AccountContext) (*PollResult, error)
}

// --- not-configured upstream: what `serve` uses before config.json is done ---

// notConfiguredUpstream is the live placeholder until the operator completes
// config.json. Every call fails with a structured, non-retryable error that
// names the missing setting — never a mock result. After the operator fills
// config.json in the platform UI, ReloadConfig swaps in the real client.
type notConfiguredUpstream struct{ missing string }

func (u *notConfiguredUpstream) Create(context.Context, *channelv1.TaskInput, *runtime.SubmitInput) (string, error) {
	return "", notConfiguredErr(u.missing)
}

func (u *notConfiguredUpstream) Poll(context.Context, string, *channelv1.AccountContext) (*PollResult, error) {
	return nil, notConfiguredErr(u.missing)
}

func notConfiguredErr(missing string) *runtime.ChannelError {
	e := runtime.NewError(runtime.CodeChannelNotConfigured, channelv1.ErrorCategory_CHANNEL_UNAVAILABLE,
		false, "config.json incomplete: missing "+missing)
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

// --- mock upstream: backs ONLY the offline `verify` subcommand -------------
//
// The mock exists so `verify` can smoke-test submit→query without credentials
// or cost. It is reachable exclusively through NewForVerify (channel.go);
// `serve` can never select it, and no config.json value routes to it.

type mockUpstream struct {
	mu    sync.Mutex
	tasks map[string]time.Time
	seq   int
}

func (m *mockUpstream) Create(context.Context, *channelv1.TaskInput, *runtime.SubmitInput) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.tasks == nil {
		m.tasks = map[string]time.Time{}
	}
	m.seq++
	id := fmt.Sprintf("mock-task-%d", m.seq)
	m.tasks[id] = time.Now()
	return id, nil
}

func (m *mockUpstream) Poll(ctx context.Context, providerTaskID string, _ *channelv1.AccountContext) (*PollResult, error) {
	// The mock reports succeeded immediately with a placeholder result.
	return &PollResult{
		RawStatus: "succeeded",
		Payload: &runtime.TaskPayload{
			ID:  providerTaskID,
			URL: []string{"https://example.invalid/mock-result.mp4"},
		},
		Progress: 100,
	}, nil
}

// failingUpstream accepts a task and then reports it as terminally failed. It
// backs the VerifyFailureProbe hook (see channel.go) so `verify` can prove this
// channel surfaces a dead task as FAILED+error instead of as a Go error.
type failingUpstream struct{ mockUpstream }

func (f *failingUpstream) Poll(ctx context.Context, providerTaskID string, _ *channelv1.AccountContext) (*PollResult, error) {
	return &PollResult{
		RawStatus: "failed",
		Err: mustFailErr(runtime.CodeContentRejected,
			"upstream task failed: content policy violation"),
	}, nil
}

func mustFailErr(code int32, msg string) *runtime.ChannelError {
	e := runtime.NewError(code, channelv1.ErrorCategory_CONTENT_REJECTED, false, msg)
	e.Stage = channelv1.ErrorStage_STAGE_QUERY
	e.Source = "UPSTREAM"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

// --- real upstream: 无痕 AI /video_removal ---------------------------------

const defaultUploadHeaders = `{"Content-Type":"application/octet-stream"}`

type realUpstream struct {
	baseURL       string
	timeout       time.Duration
	uploadURL     string
	uploadHeaders string
	method        string
	// assets is the exit for asset-library material: a file:// videos[] entry
	// has to become a URL 无痕 can reach (see assets.go).
	assets *assetBridge
	// outputs maps a provider task id to the material registered for it, so
	// Poll can report where the finished file landed and drop the registration
	// once the task is terminal.
	outputs sync.Map
}

// taskAssets is what one live task holds on to between Submit and Poll.
type taskAssets struct {
	token  string
	sink   *resultSink
	images []string
}

func newRealUpstream(cfg *runtime.Config) *realUpstream {
	timeout := time.Duration(cfg.Int("http_timeout_seconds", 60)) * time.Second
	if timeout <= 0 {
		timeout = 60 * time.Second
	}
	return &realUpstream{
		baseURL:       strings.TrimSpace(cfg.String("base_url", wuhen.DefaultBaseURL)),
		timeout:       timeout,
		uploadURL:     strings.TrimSpace(cfg.String("upload_url", "")),
		uploadHeaders: strings.TrimSpace(cfg.String("upload_headers", "")),
		method:        strings.TrimSpace(cfg.String("method", "")),
		assets:        newAssetBridge(cfg),
	}
}

func (u *realUpstream) clientFor(account *channelv1.AccountContext, stage channelv1.ErrorStage) (*wuhen.Client, error) {
	key, err := apiKeyFromAccount(account, stage)
	if err != nil {
		return nil, err
	}
	client := wuhen.NewClient(wuhen.Session{APIKey: key}, "", &http.Client{Timeout: u.timeout})
	if u.baseURL != "" {
		client.SetBaseURL(u.baseURL)
	}
	return client, nil
}

func apiKeyFromAccount(account *channelv1.AccountContext, stage channelv1.ErrorStage) (string, error) {
	if account == nil || len(account.Credential) == 0 {
		return "", accountRequiredErr(stage)
	}
	var cred accountCredential
	if err := json.Unmarshal(account.Credential, &cred); err != nil {
		return "", accountRequiredErr(stage)
	}
	key := strings.TrimSpace(cred.APIKey)
	if key == "" {
		return "", accountRequiredErr(stage)
	}
	return key, nil
}

func accountRequiredErr(stage channelv1.ErrorStage) *runtime.ChannelError {
	e := runtime.NewError(runtime.CodeAccountRequired, channelv1.ErrorCategory_AUTH_FAILED, false, "missing account credential")
	e.Stage = stage
	e.Source = "SDK"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

func (u *realUpstream) Create(ctx context.Context, input *channelv1.TaskInput, submit *runtime.SubmitInput) (string, error) {
	client, err := u.clientFor(submit.Account, channelv1.ErrorStage_STAGE_SUBMIT)
	if err != nil {
		return "", err
	}
	if input == nil {
		input = &channelv1.TaskInput{}
	}
	plan, err := planRemoval(input)
	if err != nil {
		return "", err
	}
	if plan.skipUpstream {
		return "", inputErr("图片任务不提交上游")
	}
	out := plan.upload
	headers := strings.TrimSpace(u.uploadHeaders)
	if headers == "" {
		headers = defaultUploadHeaders
	}
	method := strings.TrimSpace(u.method)
	if method == "" {
		method = wuhen.DefaultMethod
	}

	// Resolve the result target before touching the source: a missing PUT
	// destination is settled without first shipping a gigabyte of material.
	token := assetToken(submit.IdempotencyKey)
	sink, err := u.assets.sink(token, out, u.uploadURL, headers)
	if err != nil {
		return "", err
	}
	videoURL, err := u.assets.source(ctx, token, plan.video, submit.Report)
	if err != nil {
		u.assets.release(token)
		return "", err
	}

	submit.Report("UPSTREAM_CALL", "正在向上游提交去水印任务")
	id, err := client.CreateRemoval(ctx, wuhen.RemovalRequest{
		VideoURL:      videoURL,
		Model:         wuhen.UpstreamModel(submit.ModelID),
		Method:        method,
		UploadURL:     sink.putURL,
		UploadHeaders: sink.headers,
	})
	if err != nil {
		u.assets.release(token)
		return "", upstreamErr(channelv1.ErrorStage_STAGE_SUBMIT, err)
	}
	u.outputs.Store(id, &taskAssets{token: token, sink: sink, images: append([]string(nil), plan.images...)})
	submit.Report("UPSTREAM_ACCEPTED", "上游已受理任务 "+id)
	return id, nil
}

func (u *realUpstream) Poll(ctx context.Context, providerTaskID string, account *channelv1.AccountContext) (*PollResult, error) {
	client, err := u.clientFor(account, channelv1.ErrorStage_STAGE_QUERY)
	if err != nil {
		return nil, err
	}
	st, err := client.Status(ctx, providerTaskID)
	if err != nil {
		return nil, upstreamErr(channelv1.ErrorStage_STAGE_QUERY, err)
	}
	raw := strings.ToLower(strings.TrimSpace(st.Status))
	res := &PollResult{RawStatus: st.Status, Progress: int32(st.Progress)}
	if raw == "queued" {
		// 排队时上游的 progress 是前方位次，不是百分比。
		res.Progress = -1
	}
	switch {
	case wuhen.StatusDone(raw):
		res.Progress = 100
		payload, err := u.resultPayload(ctx, providerTaskID, st)
		if err != nil {
			// The task succeeded upstream but the bytes are not readable yet.
			// That is a failed poll, not a failed task: let the next tick retry.
			return nil, err
		}
		res.Payload = payload
		u.finish(providerTaskID)
	case wuhen.StatusFailed(raw):
		msg := strings.TrimSpace(st.Description)
		if msg == "" {
			msg = "上游报告任务失败"
		}
		e := runtime.NewError(runtime.CodeErrorUnknown, channelv1.ErrorCategory_ERROR_UNKNOWN,
			false, wuhen.Sanitize(msg))
		e.Stage = channelv1.ErrorStage_STAGE_QUERY
		e.Source = "UPSTREAM"
		e.UpstreamCode = st.Status
		e.UpstreamMessage = wuhen.Sanitize(msg)
		e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
		res.Err = e
		u.finish(providerTaskID)
	}
	return res, nil
}

func (u *realUpstream) resultPayload(ctx context.Context, providerTaskID string, st wuhen.Status) (*runtime.TaskPayload, error) {
	id := strings.TrimSpace(st.TaskID)
	if id == "" {
		id = providerTaskID
	}
	payload := &runtime.TaskPayload{
		ID: id,
		Metadata: map[string]any{
			"status":   st.Status,
			"metering": st.Metering,
			"credits":  st.Credits,
		},
	}
	v, ok := u.outputs.Load(providerTaskID)
	if !ok {
		// A worker restart loses the mapping; the task is still legitimately
		// done, so report it without a location rather than as a failure.
		return payload, nil
	}
	assets, ok := v.(*taskAssets)
	if !ok {
		return payload, nil
	}
	loc, err := u.assets.materialize(ctx, assets.sink)
	if err != nil {
		return nil, err
	}
	if loc != "" {
		urls := make([]string, 0, 1+len(assets.images))
		urls = append(urls, loc)
		urls = append(urls, assets.images...)
		payload.URL = urls
	}
	return payload, nil
}

// finish drops the per-task material registration once the task is terminal.
// The sink stays in outputs: a later poll of the same task must still be able
// to report where the result went.
func (u *realUpstream) finish(providerTaskID string) {
	if v, ok := u.outputs.Load(providerTaskID); ok {
		if assets, ok := v.(*taskAssets); ok {
			u.assets.release(assets.token)
		}
	}
}

// unsignedURL drops the query and fragment so a presigned PUT signature never
// reaches the platform's stored task result.
func unsignedURL(raw string) string {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String()
}

func inputErr(msg string) *runtime.ChannelError {
	e := runtime.NewError(runtime.CodeInputInvalid, channelv1.ErrorCategory_INPUT_INVALID, false, msg)
	e.Stage = channelv1.ErrorStage_STAGE_SUBMIT
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

func upstreamErr(stage channelv1.ErrorStage, err error) *runtime.ChannelError {
	msg := wuhen.Sanitize(err.Error())
	code, category, retryable, signal := classifyUpstream(msg)
	e := runtime.NewError(code, category, retryable, msg)
	e.Stage = stage
	e.Source = "UPSTREAM"
	e.UpstreamMessage = msg
	e.AccountSignal = signal
	return e
}

func classifyUpstream(msg string) (code int32, category channelv1.ErrorCategory, retryable bool, signal channelv1.AccountSignal) {
	switch {
	case strings.Contains(msg, "积分不足"):
		return runtime.CodeQuotaExceeded, channelv1.ErrorCategory_QUOTA_EXCEEDED, true, channelv1.AccountSignal_ACCOUNT_FAILURE
	case strings.Contains(msg, "API Key"),
		strings.Contains(msg, "access_token"),
		strings.Contains(msg, "401"):
		return runtime.CodeAuthFailed, channelv1.ErrorCategory_AUTH_FAILED, true, channelv1.AccountSignal_CREDENTIAL_INVALID
	case strings.Contains(msg, "参数不合法"),
		strings.Contains(msg, "缺少nonce"),
		strings.Contains(msg, "缺少t参数"):
		return runtime.CodeInputInvalid, channelv1.ErrorCategory_INPUT_INVALID, false, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	case strings.Contains(msg, "HTTP 429"),
		strings.Contains(msg, " 429"):
		return runtime.CodeRateLimited, channelv1.ErrorCategory_RATE_LIMITED, true, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	case strings.Contains(msg, "HTTP 408"),
		strings.Contains(msg, "请求失败"),
		strings.Contains(msg, "响应无法解析"),
		strings.Contains(msg, "响应过大"),
		strings.Contains(msg, "HTTP 5"),
		strings.Contains(msg, "读取原图失败"),
		strings.Contains(msg, "原图写入结果地址失败"):
		return runtime.CodeChannelUnavailable, channelv1.ErrorCategory_CHANNEL_UNAVAILABLE, true, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	default:
		return runtime.CodeUpstreamError, channelv1.ErrorCategory_ERROR_UNKNOWN, false, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	}
}
