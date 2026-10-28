package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
	wuhen "wuhen-sdk"
)

func noopReport(string, string) {}

func removalInput(video string, upload ...string) *channelv1.TaskInput {
	videos := []string{video}
	if len(upload) > 0 && upload[0] != "" {
		videos = append(videos, upload[0])
	}
	return &channelv1.TaskInput{Prompt: "watermark_removal", Videos: videos}
}

// upstreamStub stands in for api.wuhenai.com/v2. It answers the token exchange
// and lets each test decide what /video_removal and /status return.
func upstreamStub(t *testing.T, removal, status http.HandlerFunc) *httptest.Server {
	t.Helper()
	return upstreamStubWithToken(t, nil, removal, status)
}

func upstreamStubWithToken(t *testing.T, sawKey *string, removal, status http.HandlerFunc) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasPrefix(r.URL.Path, "/user/access_token"):
			if sawKey != nil {
				*sawKey = r.URL.Query().Get("api_key")
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0, "data": map[string]any{"access_token": "tok", "expired": 0},
			})
		case r.URL.Path == "/user/me":
			_ = json.NewEncoder(w).Encode(map[string]any{
				"code": 0, "data": map[string]any{"account_id": "acc", "account_name": "u", "balance": 42},
			})
		case r.URL.Path == "/video_removal" && removal != nil:
			removal(w, r)
		case r.URL.Path == "/status" && status != nil:
			status(w, r)
		default:
			http.NotFound(w, r)
		}
	}))
}

func liveChannel(t *testing.T, baseURL string) *Channel {
	t.Helper()
	cfg := configFrom(t, map[string]any{"base_url": baseURL})
	return &Channel{up: selectUpstream(cfg), config: cfg}
}

func testAccount(apiKey string) *channelv1.AccountContext {
	cred, _ := json.Marshal(accountCredential{APIKey: apiKey})
	return &channelv1.AccountContext{AccountId: "acc-1", Credential: cred}
}

// configFrom builds a Config the same way the plugin does at runtime: write the
// file, then load it, so tests exercise the real config path.
func configFrom(t *testing.T, values map[string]any) *runtime.Config {
	t.Helper()
	raw, err := json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "config.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := runtime.LoadConfigFrom(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

func TestSelectUpstreamNeedsBaseURL(t *testing.T) {
	cfg := configFrom(t, map[string]any{})
	up, ok := selectUpstream(cfg).(*notConfiguredUpstream)
	if !ok || up.missing != "base_url" {
		t.Fatalf("want missing base_url, got %T %+v", selectUpstream(cfg), up)
	}
	cfg = configFrom(t, map[string]any{"base_url": "https://api.example/v2"})
	if _, ok := selectUpstream(cfg).(*realUpstream); !ok {
		t.Fatalf("complete config must yield realUpstream, got %T", selectUpstream(cfg))
	}
}

func TestSubmitForwardsInputToUpstream(t *testing.T) {
	var got map[string]any
	srv := upstreamStub(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"task_id": "tid-9"}})
	}, nil)
	defer srv.Close()

	c := liveChannel(t, srv.URL)
	res, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelPro,
		Input:   removalInput("https://cdn.example/in.mp4", "https://tos.example/out.mp4?sig=secret"),
		Account: testAccount("k1"),
		Report:  noopReport,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.ProviderTaskID != "tid-9" || res.Status != channelv1.TaskStatus_QUEUED {
		t.Fatalf("submit result %+v", res)
	}
	if got["video_url"] != "https://cdn.example/in.mp4" {
		t.Fatalf("video_url=%v", got["video_url"])
	}
	if got["model"] != wuhen.ModelPro {
		t.Fatalf("model=%v want %s", got["model"], wuhen.ModelPro)
	}
	if got["method"] != wuhen.DefaultMethod {
		t.Fatalf("method=%v want %s", got["method"], wuhen.DefaultMethod)
	}
	if got["upload_headers"] != defaultUploadHeaders {
		t.Fatalf("upload_headers=%v", got["upload_headers"])
	}
}

func TestSubmitRejectsMissingAccount(t *testing.T) {
	c := liveChannel(t, "https://api.example/v2")
	_, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/in.mp4", "https://tos.example/out.mp4"),
		Report:  noopReport,
	})
	ce, ok := err.(*runtime.ChannelError)
	if !ok || ce.Code != runtime.CodeAccountRequired || ce.Retryable {
		t.Fatalf("err %+v", err)
	}
}

func TestSubmitRejectsMissingUploadURL(t *testing.T) {
	c := liveChannel(t, "https://api.example/v2")
	_, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/in.mp4"),
		Account: testAccount("k1"),
		Report:  noopReport,
	})
	ce, ok := err.(*runtime.ChannelError)
	if !ok {
		t.Fatalf("want *runtime.ChannelError, got %T (%v)", err, err)
	}
	if ce.Code != runtime.CodeInputInvalid || ce.Retryable {
		t.Fatalf("err %+v", ce)
	}
}

func TestSubmitFallsBackToConfigUploadURL(t *testing.T) {
	var got map[string]any
	srv := upstreamStub(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&got)
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"task_id": "tid-cfg"}})
	}, nil)
	defer srv.Close()

	cfg := configFrom(t, map[string]any{
		"base_url":   srv.URL,
		"upload_url": "https://tos.example/from-config.mp4?sig=cfg",
	})
	c := &Channel{up: selectUpstream(cfg), config: cfg}
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/in.mp4"),
		Account: testAccount("k1"),
		Report:  noopReport,
	}); err != nil {
		t.Fatal(err)
	}
	if got["upload_url"] != "https://tos.example/from-config.mp4?sig=cfg" {
		t.Fatalf("upload_url=%v", got["upload_url"])
	}
}

func TestQueryStripsSignatureFromResultURL(t *testing.T) {
	srv := upstreamStub(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"task_id": "tid-1"}})
	}, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"task_id": "tid-1", "status": "success", "progress": 100, "credits": 7},
		})
	})
	defer srv.Close()

	c := liveChannel(t, srv.URL)
	account := testAccount("k1")
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/in.mp4", "https://tos.example/out.mp4?sig=secret"),
		Account: account,
		Report:  noopReport,
	}); err != nil {
		t.Fatal(err)
	}

	res, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-1", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != channelv1.TaskStatus_SUCCEEDED || res.Progress != 100 {
		t.Fatalf("query %+v", res)
	}
	if res.Result == nil || len(res.Result.URL) != 1 {
		t.Fatalf("result %+v", res.Result)
	}
	if res.Result.URL[0] != "https://tos.example/out.mp4" {
		t.Fatalf("url=%q", res.Result.URL[0])
	}
	if strings.Contains(res.Result.URL[0], "secret") {
		t.Fatalf("result leaked the presign signature: %q", res.Result.URL[0])
	}
}

func TestQueuedProgressIsNotAPercentage(t *testing.T) {
	srv := upstreamStub(t, nil, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"task_id": "tid-2", "status": "queued", "progress": 12},
		})
	})
	defer srv.Close()

	res, err := liveChannel(t, srv.URL).Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-2", Account: testAccount("k1")})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != channelv1.TaskStatus_QUEUED {
		t.Fatalf("status=%v", res.Status)
	}
	if res.Progress != -1 {
		t.Fatalf("queued progress=%d, want -1", res.Progress)
	}
}

func TestQueryReportsTerminalFailureAsStatusNotError(t *testing.T) {
	srv := upstreamStub(t, nil, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"task_id": "tid-3", "status": "failed", "description": "视频解码失败"},
		})
	})
	defer srv.Close()

	res, err := liveChannel(t, srv.URL).Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-3", Account: testAccount("k1")})
	if err != nil {
		t.Fatalf("terminal upstream failure must not be a Go error: %v", err)
	}
	if res.Status != channelv1.TaskStatus_FAILED || res.Err == nil {
		t.Fatalf("query %+v", res)
	}
	if res.Err.Retryable {
		t.Fatal("a decoding failure is not retryable")
	}
	if !strings.Contains(res.Err.Message, "视频解码失败") {
		t.Fatalf("err message %q", res.Err.Message)
	}
}

func TestClassifyUpstreamBuckets(t *testing.T) {
	cases := []struct {
		msg       string
		code      int32
		category  channelv1.ErrorCategory
		retryable bool
		signal    channelv1.AccountSignal
	}{
		{"无痕账号积分不足", runtime.CodeQuotaExceeded, channelv1.ErrorCategory_QUOTA_EXCEEDED, true, channelv1.AccountSignal_ACCOUNT_FAILURE},
		{"账号缺少 API Key", runtime.CodeAuthFailed, channelv1.ErrorCategory_AUTH_FAILED, true, channelv1.AccountSignal_CREDENTIAL_INVALID},
		{"缺少nonce参数", runtime.CodeInputInvalid, channelv1.ErrorCategory_INPUT_INVALID, false, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE},
		{"无痕请求失败: dial tcp timeout", runtime.CodeChannelUnavailable, channelv1.ErrorCategory_CHANNEL_UNAVAILABLE, true, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE},
		{"无痕 HTTP 429", runtime.CodeRateLimited, channelv1.ErrorCategory_RATE_LIMITED, true, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE},
		{"原图写入结果地址失败: HTTP 408", runtime.CodeChannelUnavailable, channelv1.ErrorCategory_CHANNEL_UNAVAILABLE, true, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE},
		{"某种没见过的错误", runtime.CodeUpstreamError, channelv1.ErrorCategory_ERROR_UNKNOWN, false, channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE},
	}
	for _, tc := range cases {
		code, category, retryable, signal := classifyUpstream(tc.msg)
		if code != tc.code || category != tc.category || retryable != tc.retryable || signal != tc.signal {
			t.Fatalf("%q -> %v/%v/%v/%v, want %v/%v/%v/%v", tc.msg, code, category, retryable, signal, tc.code, tc.category, tc.retryable, tc.signal)
		}
	}
}

func TestRegistrationPutsAPIKeyInCredential(t *testing.T) {
	c := NewForVerify()
	in := &runtime.StartRegistrationInput{
		PlatformRegistrationID: "platform-reg-1",
		IdempotencyKey:         "register-key-1",
		Email:                  "user@example.com",
		Password:               "sk_live_testkey",
		RefreshToken:           "must-not-appear",
	}
	start, err := c.StartRegistration(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	again, err := c.StartRegistration(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if start.RegistrationTaskID != again.RegistrationTaskID {
		t.Fatal("same idempotency key must return the same registration task id")
	}
	afterRestart, err := NewForVerify().StartRegistration(context.Background(), in)
	if err != nil {
		t.Fatal(err)
	}
	if start.RegistrationTaskID != afterRestart.RegistrationTaskID {
		t.Fatal("registration task id must survive a worker restart")
	}
	if start.Status != channelv1.RegistrationStatus_REGISTRATION_RUNNING ||
		!strings.HasPrefix(start.RegistrationTaskID, registrationPrefix) {
		t.Fatalf("start result %+v", start)
	}

	got, err := c.QueryRegistration(context.Background(), &runtime.QueryRegistrationInput{RegistrationTaskID: start.RegistrationTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.RegistrationStatus_REGISTRATION_SUCCEEDED || got.Err != nil {
		t.Fatalf("registration %+v", got)
	}
	if got.ExternalAccountID == "" || got.DisplayName != in.Email || len(got.Credential) == 0 {
		t.Fatalf("incomplete account %+v", got)
	}
	if string(got.Fingerprint) != "{}" {
		t.Fatalf("fingerprint placeholder=%q", got.Fingerprint)
	}
	if strings.Contains(string(got.Credential), in.RefreshToken) {
		t.Fatal("credential leaked refresh token")
	}
	var credential accountCredential
	if err := json.Unmarshal(got.Credential, &credential); err != nil {
		t.Fatal(err)
	}
	if credential.APIKey != in.Password {
		t.Fatalf("credential %+v", credential)
	}

	other, err := c.StartRegistration(context.Background(), &runtime.StartRegistrationInput{
		IdempotencyKey: "register-key-2",
		Email:          "other@example.com",
		Password:       "sk_live_other",
	})
	if err != nil {
		t.Fatal(err)
	}
	otherAccount, err := c.QueryRegistration(context.Background(), &runtime.QueryRegistrationInput{RegistrationTaskID: other.RegistrationTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if other.RegistrationTaskID == start.RegistrationTaskID || otherAccount.ExternalAccountID == got.ExternalAccountID {
		t.Fatal("different registration keys must create different accounts")
	}
}

func TestRegistrationUsesConfigKeyWhenPasswordIsNotAPIKey(t *testing.T) {
	cfg := configFrom(t, map[string]any{"base_url": "https://api.example/v2", "api_key": "sk_live_from_config"})
	c := &Channel{up: selectUpstream(cfg), config: cfg}
	start, err := c.StartRegistration(context.Background(), &runtime.StartRegistrationInput{
		IdempotencyKey: "gmail-key",
		Email:          "user@gmail.com",
		Password:       "GoogleAccountPassword123",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.QueryRegistration(context.Background(), &runtime.QueryRegistrationInput{RegistrationTaskID: start.RegistrationTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.RegistrationStatus_REGISTRATION_SUCCEEDED {
		t.Fatalf("registration %+v", got)
	}
	var credential accountCredential
	if err := json.Unmarshal(got.Credential, &credential); err != nil {
		t.Fatal(err)
	}
	if credential.APIKey != "sk_live_from_config" {
		t.Fatalf("credential %+v", credential)
	}
	if strings.Contains(string(got.Credential), "GoogleAccountPassword123") {
		t.Fatal("google password must not enter credential")
	}
}

func TestRegistrationMissingAPIKeyFails(t *testing.T) {
	c := NewForVerify()
	start, err := c.StartRegistration(context.Background(), &runtime.StartRegistrationInput{
		IdempotencyKey: "key-only-email",
		Email:          "user@example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.QueryRegistration(context.Background(), &runtime.QueryRegistrationInput{RegistrationTaskID: start.RegistrationTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.RegistrationStatus_REGISTRATION_FAILED || got.Err == nil {
		t.Fatalf("missing API key: %+v", got)
	}
	if got.Err.Code != runtime.CodeInputInvalid || got.Err.Details["reason"] != "API_KEY_REQUIRED" {
		t.Fatalf("err %+v", got.Err)
	}
}

func TestRegistrationEmptyEmailIsRequired(t *testing.T) {
	c := NewForVerify()
	start, err := c.StartRegistration(context.Background(), &runtime.StartRegistrationInput{
		IdempotencyKey: "probe-key",
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.QueryRegistration(context.Background(), &runtime.QueryRegistrationInput{RegistrationTaskID: start.RegistrationTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.RegistrationStatus_REGISTRATION_FAILED || got.Err == nil {
		t.Fatalf("empty email must be a terminal registration failure: %+v", got)
	}
	if got.Err.Code != runtime.CodeInputInvalid || got.Err.Details["reason"] != "EMAIL_REQUIRED" || got.Err.Retryable {
		t.Fatalf("err %+v", got.Err)
	}
}

func TestSubmitUsesCredentialAPIKey(t *testing.T) {
	var tokenKey string
	srv := upstreamStubWithToken(t, &tokenKey, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"task_id": "tid-key"}})
	}, nil)
	defer srv.Close()

	_, err := liveChannel(t, srv.URL).Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/in.mp4", "https://tos.example/out.mp4"),
		Account: testAccount("from-credential"),
		Report:  noopReport,
	})
	if err != nil {
		t.Fatal(err)
	}
	if tokenKey != "from-credential" {
		t.Fatalf("upstream api_key=%q, want credential key", tokenKey)
	}
}

func TestRegistrationRejectsMissingOrUnknownTaskID(t *testing.T) {
	c := NewForVerify()
	if _, err := c.StartRegistration(context.Background(), &runtime.StartRegistrationInput{}); err == nil {
		t.Fatal("missing registration identity must fail")
	}
	for _, id := range []string{"", "another-channel-reg-1"} {
		_, err := c.QueryRegistration(context.Background(), &runtime.QueryRegistrationInput{RegistrationTaskID: id})
		ce, ok := err.(*runtime.ChannelError)
		if !ok || ce.Code != runtime.CodeInputInvalid || ce.Stage != channelv1.ErrorStage_STAGE_REGISTER_QUERY {
			t.Fatalf("id=%q err=%T %+v", id, err, err)
		}
	}
}

func TestListModelsAndCapabilitiesAgree(t *testing.T) {
	c := NewForVerify()
	models, err := c.ListModels(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	caps, err := c.Capabilities(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(models) != 2 || len(caps) != len(models) {
		t.Fatalf("models=%d caps=%d", len(models), len(caps))
	}
	seen := map[string]bool{}
	for _, m := range models {
		if m.ModelId == "" || m.ModelName == "" {
			t.Fatalf("model %+v", m)
		}
		if seen[m.ModelId] {
			t.Fatalf("duplicate model id %q", m.ModelId)
		}
		seen[m.ModelId] = true
	}
	for _, capability := range caps {
		if !seen[capability.ModelKey] {
			t.Fatalf("capability %q has no matching model", capability.ModelKey)
		}
	}
}

func TestMapStatusCoversUpstreamValues(t *testing.T) {
	cases := map[string]channelv1.TaskStatus{
		"queued":     channelv1.TaskStatus_QUEUED,
		"processing": channelv1.TaskStatus_RUNNING,
		"success":    channelv1.TaskStatus_SUCCEEDED,
		"finished":   channelv1.TaskStatus_SUCCEEDED,
		"failed":     channelv1.TaskStatus_FAILED,
		"cancelled":  channelv1.TaskStatus_CANCELLED,
		"weird":      channelv1.TaskStatus_TASK_STATUS_UNKNOWN,
	}
	for raw, want := range cases {
		if got := mapStatus(raw); got != want {
			t.Fatalf("mapStatus(%q)=%v want %v", raw, got, want)
		}
	}
}

func TestCheckTaskAlwaysEligible(t *testing.T) {
	got, err := NewForVerify().CheckTask(context.Background(), &runtime.CheckTaskInput{
		ModelID: wuhen.ModelStd,
		Input:   SampleInput,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !got.Eligible {
		t.Fatalf("check task %+v", got)
	}
}

func TestCheckAccountReportsBalance(t *testing.T) {
	srv := upstreamStub(t, nil, nil)
	defer srv.Close()

	got, err := liveChannel(t, srv.URL).CheckAccount(context.Background(), &runtime.CheckAccountInput{
		Account: testAccount("k1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Points == nil || *got.Points != 42 {
		t.Fatalf("points %+v", got)
	}
}

func TestImagePassthroughReturnsOriginalLinks(t *testing.T) {
	called := false
	srv := upstreamStub(t, func(http.ResponseWriter, *http.Request) {
		called = true
	}, func(http.ResponseWriter, *http.Request) {
		called = true
	})
	defer srv.Close()

	raw := "https://cdn.example/a.png?sig=keep"
	second := "https://cdn.example/b.webp"
	c := liveChannel(t, srv.URL)
	res, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input: &channelv1.TaskInput{
			Prompt: "watermark_removal",
			Images: []string{raw, "  ", second, raw},
		},
		Report: noopReport,
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != channelv1.TaskStatus_QUEUED || !strings.HasPrefix(res.ProviderTaskID, imagePassthroughPrefix) {
		t.Fatalf("submit %+v", res)
	}

	got, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: res.ProviderTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.TaskStatus_SUCCEEDED || got.Progress != 100 {
		t.Fatalf("query %+v", got)
	}
	if got.Result == nil || len(got.Result.URL) != 2 || got.Result.URL[0] != raw || got.Result.URL[1] != second {
		t.Fatalf("urls %+v", got.Result)
	}
	if called {
		t.Fatal("image task must not call upstream")
	}

	restarted := liveChannel(t, srv.URL)
	again, err := restarted.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: res.ProviderTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if again.Result == nil || len(again.Result.URL) != 2 || again.Result.URL[0] != raw {
		t.Fatalf("restart lost the original links: %+v", again.Result)
	}
}

func TestImageLandsOriginalBytesOnUploadTarget(t *testing.T) {
	var putBody []byte
	var putType string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/in.png":
			_, _ = w.Write([]byte("PNGDATA"))
		case "/out.png":
			if r.Method != http.MethodPut {
				t.Errorf("method %s", r.Method)
			}
			putType = r.Header.Get("Content-Type")
			putBody, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	c := &Channel{up: &notConfiguredUpstream{missing: "base_url"}}
	res, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID:        wuhen.ModelStd,
		IdempotencyKey: "image-land-1",
		Input:          removalInput(srv.URL+"/in.png", srv.URL+"/out.png?sig=secret"),
		Report:         noopReport,
	})
	if err != nil {
		t.Fatal(err)
	}
	if string(putBody) != "PNGDATA" {
		t.Fatalf("landed %q", putBody)
	}
	if putType != "image/png" {
		t.Fatalf("content-type %q", putType)
	}
	got, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: res.ProviderTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.TaskStatus_SUCCEEDED || got.Result == nil || len(got.Result.URL) != 1 {
		t.Fatalf("query %+v", got)
	}
	if got.Result.URL[0] != srv.URL+"/out.png" {
		t.Fatalf("url=%q", got.Result.URL[0])
	}
	if strings.Contains(got.Result.URL[0], "sig=") {
		t.Fatalf("result leaked the presign signature: %q", got.Result.URL[0])
	}
}

func TestImageInVideosFieldSkipsRemoval(t *testing.T) {
	c := &Channel{up: &notConfiguredUpstream{missing: "base_url"}}
	res, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/shot.jpeg"),
		Report:  noopReport,
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: res.ProviderTaskID})
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != channelv1.TaskStatus_SUCCEEDED || got.Result == nil || len(got.Result.URL) != 1 {
		t.Fatalf("query %+v", got)
	}
	if got.Result.URL[0] != "https://cdn.example/shot.jpeg" {
		t.Fatalf("url=%q", got.Result.URL[0])
	}
}

func TestExtensionlessURLStaysAVideo(t *testing.T) {
	c := liveChannel(t, "https://api.example/v2")
	_, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   removalInput("https://cdn.example/media"),
		Account: testAccount("k1"),
		Report:  noopReport,
	})
	ce, ok := err.(*runtime.ChannelError)
	if !ok || ce.Code != runtime.CodeInputInvalid {
		t.Fatalf("an address without an image suffix must still be a video: %+v", err)
	}
}

func TestVideoResultKeepsOriginalImageLinks(t *testing.T) {
	srv := upstreamStub(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"task_id": "tid-mix"}})
	}, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"task_id": "tid-mix", "status": "success", "progress": 100},
		})
	})
	defer srv.Close()

	c := liveChannel(t, srv.URL)
	account := testAccount("k1")
	input := removalInput("https://cdn.example/in.mp4", "https://tos.example/out.mp4?sig=secret")
	input.Images = []string{"https://cdn.example/cover.png?sig=img"}
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   input,
		Account: account,
		Report:  noopReport,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-mix", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	if got.Result == nil || len(got.Result.URL) != 2 {
		t.Fatalf("result %+v", got.Result)
	}
	if got.Result.URL[0] != "https://tos.example/out.mp4" {
		t.Fatalf("video url=%q", got.Result.URL[0])
	}
	if got.Result.URL[1] != "https://cdn.example/cover.png?sig=img" {
		t.Fatalf("image url=%q", got.Result.URL[1])
	}
}

func TestTooManyImagesRejected(t *testing.T) {
	images := make([]string, maxPassthroughImages+1)
	for i := range images {
		images[i] = fmt.Sprintf("https://cdn.example/%d.png", i)
	}
	c := liveChannel(t, "https://api.example/v2")
	_, err := c.Submit(context.Background(), &runtime.SubmitInput{
		ModelID: wuhen.ModelStd,
		Input:   &channelv1.TaskInput{Prompt: "watermark_removal", Images: images},
		Report:  noopReport,
	})
	ce, ok := err.(*runtime.ChannelError)
	if !ok || ce.Code != runtime.CodeInputInvalid || ce.Retryable {
		t.Fatalf("err %+v", err)
	}
	got, checkErr := c.CheckTask(context.Background(), &runtime.CheckTaskInput{
		ModelID: wuhen.ModelStd,
		Input:   &channelv1.TaskInput{Prompt: "watermark_removal", Images: images},
	})
	if checkErr != nil || got.Eligible || !strings.Contains(got.Reason, "超过") {
		t.Fatalf("check %+v (%v)", got, checkErr)
	}
}

func TestCheckTaskAllowsImageWithoutAssetExit(t *testing.T) {
	bare := channelWith(t, map[string]any{"base_url": "https://api.example/v2"})
	got, err := bare.CheckTask(context.Background(), &runtime.CheckTaskInput{
		ModelID: wuhen.ModelStd,
		Input:   &channelv1.TaskInput{Prompt: "watermark_removal", Images: []string{"file:///tmp/no-such-cover.png"}},
	})
	if err != nil || !got.Eligible {
		t.Fatalf("image passthrough must skip the local-video preflight: %+v (%v)", got, err)
	}
}

func TestCorruptImagePassthroughIsTerminal(t *testing.T) {
	got, err := liveChannel(t, "https://api.example/v2").Query(context.Background(), &runtime.QueryInput{
		ProviderTaskID: imagePassthroughPrefix + "!!!",
	})
	if err != nil {
		t.Fatalf("corrupt passthrough must not be a Go error: %v", err)
	}
	if got.Status != channelv1.TaskStatus_FAILED || got.Err == nil || got.Err.Retryable {
		t.Fatalf("query %+v", got)
	}
}

func TestCheckAccountMissingCredential(t *testing.T) {
	_, err := liveChannel(t, "https://api.example/v2").CheckAccount(context.Background(), &runtime.CheckAccountInput{})
	ce, ok := err.(*runtime.ChannelError)
	if !ok || ce.Code != runtime.CodeAccountRequired {
		t.Fatalf("err %+v", err)
	}
}
