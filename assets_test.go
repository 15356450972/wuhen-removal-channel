package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
	wuhen "wuhen-sdk"
)

func channelWith(t *testing.T, values map[string]any) *Channel {
	t.Helper()
	cfg := configFrom(t, values)
	return &Channel{up: selectUpstream(cfg), config: cfg}
}

// freeAddr picks a port for the built-in file server. The asset server registry
// is keyed by address, so a fresh port keeps tests from sharing a listener.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}
	return addr
}

func writeAsset(t *testing.T, path, body string) string {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// acceptRemoval answers /video_removal, recording the body the channel sent.
func acceptRemoval(t *testing.T, got *map[string]any, taskID string) http.HandlerFunc {
	t.Helper()
	return func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(got); err != nil {
			t.Errorf("decode removal body: %v", err)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"task_id": taskID}})
	}
}

func reportSuccess(taskID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"code": 0,
			"data": map[string]any{"task_id": taskID, "status": "success", "progress": 100, "metering": 3},
		})
	}
}

func httpDo(t *testing.T, method, url, body string) (int, string) {
	t.Helper()
	var payload io.Reader
	if body != "" {
		payload = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, payload)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp.StatusCode, string(raw)
}

// fakeStorage is an S3-compatible bucket: path style, SigV4 either in the
// Authorization header (our uploads) or in the query (the presigned URLs we
// hand 无痕).
type fakeStorage struct {
	srv *httptest.Server

	mu       sync.Mutex
	objects  map[string]string
	presigns []string
	headers  []string
}

func newFakeStorage(t *testing.T) *fakeStorage {
	t.Helper()
	s := &fakeStorage{objects: map[string]string{}}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/bucket-1/")
		switch r.Method {
		case http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, "read", http.StatusBadRequest)
				return
			}
			s.mu.Lock()
			s.objects[key] = string(body)
			if sig := r.URL.Query().Get("X-Amz-Signature"); sig != "" {
				s.presigns = append(s.presigns, key)
			} else {
				s.headers = append(s.headers, r.Header.Get("Authorization"))
			}
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
		case http.MethodGet:
			s.mu.Lock()
			body, ok := s.objects[key]
			s.mu.Unlock()
			if !ok {
				http.Error(w, "NoSuchKey", http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, body)
		default:
			http.Error(w, "method", http.StatusMethodNotAllowed)
		}
	}))
	t.Cleanup(s.srv.Close)
	return s
}

func (s *fakeStorage) config(extra map[string]any) map[string]any {
	cfg := map[string]any{
		"storage_endpoint":   s.srv.URL,
		"storage_region":     "cn-beijing",
		"storage_bucket":     "bucket-1",
		"storage_access_key": "AKIAEXAMPLE",
		"storage_secret_key": "secret-example",
		"storage_path_style": true,
	}
	for k, v := range extra {
		cfg[k] = v
	}
	return cfg
}

func (s *fakeStorage) object(key string) (string, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	body, ok := s.objects[key]
	return body, ok
}

func TestLocalPathFromURLAcceptsOnlyLocalAbsolutePaths(t *testing.T) {
	ok := map[string]string{
		"file:///var/lib/mao/in.mp4":           "/var/lib/mao/in.mp4",
		"file://localhost/var/lib/mao/in.mp4":  "/var/lib/mao/in.mp4",
		"file:///var/lib/mao/with%20space.mp4": "/var/lib/mao/with space.mp4",
		"file:///var/lib/mao/./in.mp4":         "/var/lib/mao/in.mp4",
	}
	for raw, want := range ok {
		if !isFileURL(raw) {
			t.Fatalf("%q should be recognized as a local material", raw)
		}
		got, err := localPathFromURL(raw)
		if err != nil || got != want {
			t.Fatalf("localPathFromURL(%q) = %q, %v; want %q", raw, got, err, want)
		}
	}
	for _, raw := range []string{"file://nas.example/share/in.mp4", "file://in.mp4", "https://cdn.example/in.mp4"} {
		if _, err := localPathFromURL(raw); err == nil {
			t.Fatalf("%q must be rejected", raw)
		}
	}
	if isFileURL("https://cdn.example/in.mp4") {
		t.Fatal("an http material must not look local")
	}
}

func TestFileURLRoundTripsThroughLocalPath(t *testing.T) {
	for _, p := range []string{"/var/lib/mao/in.mp4", "/var/lib/mao/带空格 的.mp4"} {
		got, err := localPathFromURL(fileURL(p))
		if err != nil || got != p {
			t.Fatalf("round trip %q -> %q (%v)", p, got, err)
		}
	}
}

// The built-in file server is the fallback exit: 无痕 GETs the source from it
// and PUTs the finished video straight back onto this filesystem.
func TestLocalMaterialServedAndResultWrittenBackByFileServer(t *testing.T) {
	dir := t.TempDir()
	source := writeAsset(t, filepath.Join(dir, "with_wm.mp4"), "SOURCE-BYTES")
	dest := filepath.Join(dir, "results", "nowm.mp4")

	var sent map[string]any
	upstream := upstreamStub(t, acceptRemoval(t, &sent, "tid-local"), reportSuccess("tid-local"))
	defer upstream.Close()

	addr := freeAddr(t)
	c := channelWith(t, map[string]any{
		"base_url":              upstream.URL,
		"asset_serve_addr":      addr,
		"asset_public_base_url": "http://" + addr,
	})
	account := testAccount("k1")
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		IdempotencyKey: "task-local-1",
		ModelID:        wuhen.ModelStd,
		Input:          removalInput(fileURL(source), fileURL(dest)),
		Account:        account,
		Report:         noopReport,
	}); err != nil {
		t.Fatal(err)
	}

	videoURL, _ := sent["video_url"].(string)
	if !strings.HasPrefix(videoURL, "http://"+addr+assetSourcePath) {
		t.Fatalf("video_url=%q, want the built-in file server", videoURL)
	}
	if code, body := httpDo(t, http.MethodGet, videoURL, ""); code != http.StatusOK || body != "SOURCE-BYTES" {
		t.Fatalf("upstream fetch of the source: %d %q", code, body)
	}

	uploadURL, _ := sent["upload_url"].(string)
	if !strings.HasPrefix(uploadURL, "http://"+addr+assetSinkPath) {
		t.Fatalf("upload_url=%q, want the built-in file server", uploadURL)
	}
	if code, _ := httpDo(t, http.MethodPut, uploadURL, "RESULT-BYTES"); code != http.StatusOK {
		t.Fatalf("upstream PUT of the result: %d", code)
	}

	res, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-local", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	if res.Status != channelv1.TaskStatus_SUCCEEDED || res.Result == nil || len(res.Result.URL) != 1 {
		t.Fatalf("query %+v", res)
	}
	if res.Result.URL[0] != fileURL(dest) {
		t.Fatalf("result url=%q, want %q", res.Result.URL[0], fileURL(dest))
	}
	landed, err := os.ReadFile(dest)
	if err != nil || string(landed) != "RESULT-BYTES" {
		t.Fatalf("result file %q (%v)", landed, err)
	}

	// The registration is dropped once the task is terminal.
	if code, _ := httpDo(t, http.MethodGet, videoURL, ""); code != http.StatusNotFound {
		t.Fatalf("a finished task must stop serving its source, got %d", code)
	}
}

func TestFileServerRefusesUnknownTokenAndWrongMethod(t *testing.T) {
	addr := freeAddr(t)
	c := channelWith(t, map[string]any{
		"base_url":              "https://api.example/v2",
		"asset_serve_addr":      addr,
		"asset_public_base_url": "http://" + addr,
	})
	server := c.up.(*realUpstream).assets.local
	if _, err := server.publishSource("token-1", writeAsset(t, filepath.Join(t.TempDir(), "a.mp4"), "x")); err != nil {
		t.Fatal(err)
	}
	if code, _ := httpDo(t, http.MethodGet, "http://"+addr+assetSourcePath+"token-unknown", ""); code != http.StatusNotFound {
		t.Fatalf("unknown token=%d, want 404", code)
	}
	if code, _ := httpDo(t, http.MethodDelete, "http://"+addr+assetSourcePath+"token-1", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("DELETE on a source=%d, want 405", code)
	}
	if code, _ := httpDo(t, http.MethodGet, "http://"+addr+assetSinkPath+"token-1", ""); code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on a sink=%d, want 405", code)
	}
}

// Object storage is the preferred exit: the material is uploaded, and 无痕 gets
// a readable URL plus a presigned PUT for the result.
func TestLocalMaterialUploadedToObjectStorage(t *testing.T) {
	storage := newFakeStorage(t)
	source := writeAsset(t, filepath.Join(t.TempDir(), "with_wm.mp4"), "SOURCE-BYTES")

	var sent map[string]any
	upstream := upstreamStub(t, acceptRemoval(t, &sent, "tid-oss"), reportSuccess("tid-oss"))
	defer upstream.Close()

	c := channelWith(t, storage.config(map[string]any{
		"base_url":                upstream.URL,
		"storage_public_base_url": "https://cdn.example",
		"storage_prefix":          "wuhen",
	}))
	account := testAccount("k1")
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		IdempotencyKey: "task-oss-1",
		ModelID:        wuhen.ModelStd,
		Input:          removalInput(fileURL(source)),
		Account:        account,
		Report:         noopReport,
	}); err != nil {
		t.Fatal(err)
	}

	videoURL, _ := sent["video_url"].(string)
	if !strings.HasPrefix(videoURL, "https://cdn.example/wuhen/in/") {
		t.Fatalf("video_url=%q, want the bucket's public domain", videoURL)
	}
	key := strings.TrimPrefix(videoURL, "https://cdn.example/")
	if body, ok := storage.object(key); !ok || body != "SOURCE-BYTES" {
		t.Fatalf("object %q = %q (present=%v)", key, body, ok)
	}
	storage.mu.Lock()
	auth := append([]string(nil), storage.headers...)
	storage.mu.Unlock()
	if len(auth) != 1 || !strings.HasPrefix(auth[0], sigV4Algorithm+" Credential=AKIAEXAMPLE/") {
		t.Fatalf("upload authorization %q", auth)
	}

	uploadURL, _ := sent["upload_url"].(string)
	if !strings.Contains(uploadURL, "X-Amz-Signature=") || !strings.Contains(uploadURL, "/wuhen/out/") {
		t.Fatalf("upload_url=%q, want a presigned PUT into the bucket", uploadURL)
	}
	if code, body := httpDo(t, http.MethodPut, uploadURL, "RESULT-BYTES"); code != http.StatusOK {
		t.Fatalf("presigned PUT: %d %q", code, body)
	}

	res, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-oss", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || len(res.Result.URL) != 1 {
		t.Fatalf("query %+v", res)
	}
	if !strings.HasPrefix(res.Result.URL[0], "https://cdn.example/wuhen/out/") {
		t.Fatalf("result url=%q", res.Result.URL[0])
	}
	if strings.Contains(res.Result.URL[0], "X-Amz-Signature") {
		t.Fatalf("result leaked a signature: %q", res.Result.URL[0])
	}
}

// With storage but no inbound exit, a file:// result target means the upstream
// PUTs into the bucket and Query copies the object down to the asked-for path.
func TestStorageResultCopiedDownToLocalPath(t *testing.T) {
	storage := newFakeStorage(t)
	dir := t.TempDir()
	source := writeAsset(t, filepath.Join(dir, "with_wm.mp4"), "SOURCE-BYTES")
	dest := filepath.Join(dir, "results", "nowm.mp4")

	var sent map[string]any
	upstream := upstreamStub(t, acceptRemoval(t, &sent, "tid-down"), reportSuccess("tid-down"))
	defer upstream.Close()

	c := channelWith(t, storage.config(map[string]any{"base_url": upstream.URL}))
	account := testAccount("k1")
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		IdempotencyKey: "task-down-1",
		ModelID:        wuhen.ModelStd,
		Input:          removalInput(fileURL(source), fileURL(dest)),
		Account:        account,
		Report:         noopReport,
	}); err != nil {
		t.Fatal(err)
	}

	// No public base URL configured, so even the source is a presigned GET.
	videoURL, _ := sent["video_url"].(string)
	if !strings.Contains(videoURL, "X-Amz-Signature=") {
		t.Fatalf("video_url=%q, want a presigned GET", videoURL)
	}
	if code, body := httpDo(t, http.MethodGet, videoURL, ""); code != http.StatusOK || body != "SOURCE-BYTES" {
		t.Fatalf("presigned GET of the source: %d %q", code, body)
	}

	uploadURL, _ := sent["upload_url"].(string)
	if code, _ := httpDo(t, http.MethodPut, uploadURL, "RESULT-BYTES"); code != http.StatusOK {
		t.Fatalf("presigned PUT of the result: %d", code)
	}

	res, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-down", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || len(res.Result.URL) != 1 || res.Result.URL[0] != fileURL(dest) {
		t.Fatalf("query %+v", res.Result)
	}
	landed, err := os.ReadFile(dest)
	if err != nil || string(landed) != "RESULT-BYTES" {
		t.Fatalf("result file %q (%v)", landed, err)
	}
}

// A result that has not landed yet is a poll failure, never a completed task
// with a half-written file behind it.
func TestQueryRetriesWhileLocalResultIsMissing(t *testing.T) {
	dir := t.TempDir()
	source := writeAsset(t, filepath.Join(dir, "with_wm.mp4"), "SOURCE-BYTES")
	dest := filepath.Join(dir, "nowm.mp4")

	var sent map[string]any
	upstream := upstreamStub(t, acceptRemoval(t, &sent, "tid-pending"), reportSuccess("tid-pending"))
	defer upstream.Close()

	addr := freeAddr(t)
	c := channelWith(t, map[string]any{
		"base_url":              upstream.URL,
		"asset_serve_addr":      addr,
		"asset_public_base_url": "http://" + addr,
	})
	account := testAccount("k1")
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		IdempotencyKey: "task-pending-1",
		ModelID:        wuhen.ModelStd,
		Input:          removalInput(fileURL(source), fileURL(dest)),
		Account:        account,
		Report:         noopReport,
	}); err != nil {
		t.Fatal(err)
	}

	_, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-pending", Account: account})
	ce, ok := err.(*runtime.ChannelError)
	if !ok {
		t.Fatalf("want *runtime.ChannelError, got %T (%v)", err, err)
	}
	if !ce.Retryable || ce.Stage != channelv1.ErrorStage_STAGE_QUERY {
		t.Fatalf("a not-yet-written result must be a retryable query failure: %+v", ce)
	}

	if code, _ := httpDo(t, http.MethodPut, sent["upload_url"].(string), "RESULT-BYTES"); code != http.StatusOK {
		t.Fatal("upstream PUT failed")
	}
	res, err := c.Query(context.Background(), &runtime.QueryInput{ProviderTaskID: "tid-pending", Account: account})
	if err != nil {
		t.Fatal(err)
	}
	if res.Result == nil || len(res.Result.URL) != 1 || res.Result.URL[0] != fileURL(dest) {
		t.Fatalf("query after the PUT: %+v", res.Result)
	}
}

func TestLocalMaterialWithoutAnyExitIsNotConfigured(t *testing.T) {
	source := writeAsset(t, filepath.Join(t.TempDir(), "with_wm.mp4"), "SOURCE-BYTES")
	c := channelWith(t, map[string]any{
		"base_url":   "https://api.example/v2",
		"upload_url": "https://tos.example/out.mp4?sig=cfg",
	})
	_, err := c.Submit(context.Background(), &runtime.SubmitInput{
		IdempotencyKey: "task-noexit",
		ModelID:        wuhen.ModelStd,
		Input:          removalInput(fileURL(source)),
		Account:        testAccount("k1"),
		Report:         noopReport,
	})
	ce, ok := err.(*runtime.ChannelError)
	if !ok || ce.Code != runtime.CodeChannelNotConfigured || ce.Retryable {
		t.Fatalf("err %T %+v", err, err)
	}
	if !strings.Contains(ce.Message, "storage_endpoint") || !strings.Contains(ce.Message, "asset_public_base_url") {
		t.Fatalf("the error must name the missing settings: %q", ce.Message)
	}
}

func TestCheckTaskPreflightsLocalMaterial(t *testing.T) {
	dir := t.TempDir()
	present := writeAsset(t, filepath.Join(dir, "in.mp4"), "SOURCE-BYTES")
	empty := writeAsset(t, filepath.Join(dir, "empty.mp4"), "")
	addr := freeAddr(t)
	configured := channelWith(t, map[string]any{
		"base_url":              "https://api.example/v2",
		"asset_serve_addr":      addr,
		"asset_public_base_url": "http://" + addr,
	})

	got, err := configured.CheckTask(context.Background(), &runtime.CheckTaskInput{
		ModelID: wuhen.ModelStd, Input: removalInput(fileURL(present)),
	})
	if err != nil || !got.Eligible {
		t.Fatalf("a readable local material must be eligible: %+v (%v)", got, err)
	}

	for _, tc := range []struct {
		name  string
		video string
	}{
		{"missing", fileURL(filepath.Join(dir, "nope.mp4"))},
		{"empty", fileURL(empty)},
		{"remote host", "file://nas.example/share/in.mp4"},
	} {
		got, err := configured.CheckTask(context.Background(), &runtime.CheckTaskInput{
			ModelID: wuhen.ModelStd, Input: removalInput(tc.video),
		})
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if got.Eligible || strings.TrimSpace(got.Reason) == "" {
			t.Fatalf("%s must be rejected with a reason: %+v", tc.name, got)
		}
	}

	bare := channelWith(t, map[string]any{"base_url": "https://api.example/v2"})
	got, err = bare.CheckTask(context.Background(), &runtime.CheckTaskInput{
		ModelID: wuhen.ModelStd, Input: removalInput(fileURL(present)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Eligible || !strings.Contains(got.Reason, "asset_public_base_url") {
		t.Fatalf("a channel with no exit must decline: %+v", got)
	}
}

func TestRemoteMaterialStillBypassesTheAssetBridge(t *testing.T) {
	storage := newFakeStorage(t)
	var sent map[string]any
	upstream := upstreamStub(t, acceptRemoval(t, &sent, "tid-remote"), nil)
	defer upstream.Close()

	c := channelWith(t, storage.config(map[string]any{"base_url": upstream.URL}))
	if _, err := c.Submit(context.Background(), &runtime.SubmitInput{
		IdempotencyKey: "task-remote",
		ModelID:        wuhen.ModelStd,
		Input:          removalInput("https://cdn.example/in.mp4", "https://tos.example/out.mp4?sig=secret"),
		Account:        testAccount("k1"),
		Report:         noopReport,
	}); err != nil {
		t.Fatal(err)
	}
	if sent["video_url"] != "https://cdn.example/in.mp4" {
		t.Fatalf("a public source must be forwarded verbatim: %v", sent["video_url"])
	}
	if sent["upload_url"] != "https://tos.example/out.mp4?sig=secret" {
		t.Fatalf("a caller-supplied PUT target must be forwarded verbatim: %v", sent["upload_url"])
	}
	storage.mu.Lock()
	uploads := len(storage.objects)
	storage.mu.Unlock()
	if uploads != 0 {
		t.Fatalf("a remote material must not touch object storage, %d objects written", uploads)
	}
}

func TestAssetTokenIsStablePerIdempotencyKey(t *testing.T) {
	if assetToken("task-1") != assetToken("task-1") {
		t.Fatal("a retried submit must reuse the same asset token")
	}
	if assetToken("task-1") == assetToken("task-2") {
		t.Fatal("different tasks must not share an asset token")
	}
	if assetToken("") == assetToken("") {
		t.Fatal("without an idempotency key the token must be random")
	}
}

func TestObjectKeyAndPathEncoding(t *testing.T) {
	if got := encodeObjectPath("wuhen/in/a b:c=d/名字.mp4"); strings.ContainsAny(got, " :=") {
		t.Fatalf("SigV4 requires every reserved character escaped, got %q", got)
	}
	if got := encodeObjectPath("wuhen/in/plain.mp4"); got != "wuhen/in/plain.mp4" {
		t.Fatalf("unreserved characters must survive, got %q", got)
	}
	store := &objectStore{prefix: "wuhen"}
	if key := store.key("in", "tok", ".MP4"); !strings.HasPrefix(key, "wuhen/in/") || !strings.HasSuffix(key, "tok.mp4") {
		t.Fatalf("key=%q", key)
	}
	if key := store.key("out", "tok", "/etc/passwd"); !strings.HasSuffix(key, "tok.mp4") {
		t.Fatalf("a bogus extension must fall back to .mp4, got %q", key)
	}
}

func TestScrubStorageHidesSignatures(t *testing.T) {
	raw := `Put "https://b.tos.example/k?X-Amz-Credential=AKIA%2F20260903&X-Amz-Signature=deadbeef": timeout`
	got := scrubStorage(raw)
	if strings.Contains(got, "deadbeef") || strings.Contains(got, "AKIA") {
		t.Fatalf("scrubbed message still carries credentials: %q", got)
	}
	if !strings.Contains(got, "timeout") {
		t.Fatalf("scrubbing must keep the diagnosis: %q", got)
	}
}

func TestWriteStreamPublishesAtomically(t *testing.T) {
	dest := filepath.Join(t.TempDir(), "out.mp4")
	if err := writeStream(dest, bytes.NewReader([]byte("BYTES"))); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dest)
	if err != nil || string(got) != "BYTES" {
		t.Fatalf("file %q (%v)", got, err)
	}
	if err := writeStream(dest, strings.NewReader("")); err == nil {
		t.Fatal("an empty payload must be rejected")
	}
	got, err = os.ReadFile(dest)
	if err != nil || string(got) != "BYTES" {
		t.Fatalf("a rejected write must leave the previous file intact: %q (%v)", got, err)
	}
	entries, err := os.ReadDir(filepath.Dir(dest))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("temp files leaked: %v", entries)
	}
}
