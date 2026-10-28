// Package main: assets.go bridges the platform's asset library to 无痕. With the
// library enabled the platform downloads and dedupes a task's material first,
// so videos[] arrives as a file:// path on this worker's own filesystem. 无痕
// has no upload endpoint — it GETs the source and PUTs the result itself — so a
// local path has to become a URL the upstream can reach. Two exits exist:
// object storage (preferred, see storage.go) and the built-in file server here
// (fallback, needs config.json asset_public_base_url to be reachable from the
// internet). With neither configured, a local material is a structured
// CHANNEL_NOT_CONFIGURED error rather than a silent failure.
package main

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
)

const (
	// maxAssetBytes caps both what we publish and what we accept back, matching
	// the 1 GiB ceiling leonardo-go uses for the same upstream.
	maxAssetBytes   = int64(1) << 30
	assetSourcePath = "/asset/in/"
	assetSinkPath   = "/asset/out/"
	assetHeartbeat  = 45 * time.Second
)

// isFileURL reports whether a videos[] entry is an asset-library local path.
func isFileURL(raw string) bool {
	return strings.HasPrefix(strings.ToLower(strings.TrimSpace(raw)), "file://")
}

// localPathFromURL resolves a file:// material to an absolute filesystem path.
// Only the local forms are accepted (file:///abs/path, file://localhost/abs/path):
// a remote host would be a network mount we cannot reason about.
func localPathFromURL(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	parsed, err := url.Parse(raw)
	if err != nil || !strings.EqualFold(parsed.Scheme, "file") {
		return "", fmt.Errorf("不是有效的 file:// 地址")
	}
	if host := parsed.Host; host != "" && !strings.EqualFold(host, "localhost") {
		return "", fmt.Errorf("file:// 只支持本机路径")
	}
	if parsed.Path == "" || !filepath.IsAbs(parsed.Path) {
		return "", fmt.Errorf("file:// 路径必须是绝对路径")
	}
	return filepath.Clean(parsed.Path), nil
}

// fileURL is the inverse of localPathFromURL, used to report a result that
// landed on this worker's filesystem.
func fileURL(p string) string {
	return (&url.URL{Scheme: "file", Path: p}).String()
}

// assetToken derives the per-task asset id from the submit idempotency key, so
// a retried submit reuses the same object key and the same served URL instead
// of uploading the material twice.
func assetToken(idempotencyKey string) string {
	key := strings.TrimSpace(idempotencyKey)
	if key == "" {
		buf := make([]byte, 12)
		if _, err := rand.Read(buf); err == nil {
			return hex.EncodeToString(buf)
		}
		key = fmt.Sprint(time.Now().UnixNano())
	}
	sum := sha256.Sum256([]byte("wuhen-removal/asset/v1\x00" + key))
	return hex.EncodeToString(sum[:12])
}

// resultSink is where the finished video goes: putURL is what 无痕 receives,
// resultURL is what the platform is told once the task succeeds. localDest is
// set when the caller asked for the result on this filesystem; objectKey is set
// when the PUT lands in object storage first.
type resultSink struct {
	putURL    string
	headers   string
	resultURL string
	localDest string
	objectKey string
}

// assetBridge is the configured set of exits for local material. Both members
// may be nil: that is the "no exit configured" state, reported as a structured
// error the moment a task actually needs one.
type assetBridge struct {
	store *objectStore
	local *localAssetServer
}

func newAssetBridge(cfg *runtime.Config) *assetBridge {
	if cfg == nil {
		return &assetBridge{}
	}
	return &assetBridge{store: newObjectStore(cfg), local: newLocalAssetServer(cfg)}
}

// available reports whether a local file:// material can be handed to 无痕 at all.
func (a *assetBridge) available() bool {
	return a != nil && (a.store != nil || a.local != nil)
}

// missing names the config.json settings an operator has to complete before
// local material works. It never names a credential value, only its key.
func (a *assetBridge) missing() string {
	return "storage_endpoint/storage_region/storage_bucket/storage_access_key/storage_secret_key 或 asset_public_base_url"
}

// source turns videos[0] into a URL 无痕 can GET. A non-file:// material is
// forwarded verbatim: the caller already gave us something public.
func (a *assetBridge) source(ctx context.Context, token, raw string, report func(string, string)) (string, error) {
	raw = strings.TrimSpace(raw)
	if !isFileURL(raw) {
		return raw, nil
	}
	local, err := localPathFromURL(raw)
	if err != nil {
		return "", inputErr("videos[0] " + err.Error())
	}
	if err := statAsset(local); err != nil {
		return "", inputErr("videos[0] " + err.Error())
	}
	switch {
	case a != nil && a.store != nil:
		key := a.store.key("in", token, filepath.Ext(local))
		if err := withHeartbeat(ctx, report, "ASSET_UPLOAD", "正在上传本地素材到对象存储", func(ctx context.Context) error {
			return a.store.put(ctx, key, local)
		}); err != nil {
			return "", storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "上传本地素材失败", err)
		}
		url, err := a.store.readableURL(key)
		if err != nil {
			return "", storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "生成素材读取地址失败", err)
		}
		return url, nil
	case a != nil && a.local != nil:
		url, err := a.local.publishSource(token, local)
		if err != nil {
			return "", storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "发布本地素材失败", err)
		}
		return url, nil
	default:
		return "", notConfiguredErr(a.missing())
	}
}

// sink decides where 无痕 PUTs the finished video. Precedence: an explicit
// videos[1], then config.json upload_url, then object storage. A file://
// videos[1] means the platform wants the result back on this filesystem.
func (a *assetBridge) sink(token, raw, configUploadURL, headers string) (*resultSink, error) {
	raw = strings.TrimSpace(raw)
	if isFileURL(raw) {
		dest, err := localPathFromURL(raw)
		if err != nil {
			return nil, inputErr("videos[1] " + err.Error())
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return nil, inputErr("videos[1] 所在目录不可写")
		}
		switch {
		// A local result prefers the file server even when storage exists: the
		// upstream PUT lands on the destination directly, with no detour
		// through the bucket and back down again.
		case a != nil && a.local != nil:
			put, err := a.local.publishSink(token, dest)
			if err != nil {
				return nil, storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "发布成片接收地址失败", err)
			}
			return &resultSink{putURL: put, headers: headers, resultURL: fileURL(dest), localDest: dest}, nil
		case a != nil && a.store != nil:
			// No inbound exit, so the upstream PUTs into object storage and
			// Poll copies the object down to the requested path.
			key := a.store.key("out", token, filepath.Ext(dest))
			put, err := a.store.presign(http.MethodPut, key)
			if err != nil {
				return nil, storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "生成成片上传地址失败", err)
			}
			return &resultSink{putURL: put, headers: headers, resultURL: fileURL(dest), localDest: dest, objectKey: key}, nil
		default:
			return nil, notConfiguredErr(a.missing())
		}
	}
	if raw != "" {
		return &resultSink{putURL: raw, headers: headers, resultURL: unsignedURL(raw)}, nil
	}
	if configUploadURL = strings.TrimSpace(configUploadURL); configUploadURL != "" {
		return &resultSink{putURL: configUploadURL, headers: headers, resultURL: unsignedURL(configUploadURL)}, nil
	}
	if a != nil && a.store != nil {
		key := a.store.key("out", token, ".mp4")
		put, err := a.store.presign(http.MethodPut, key)
		if err != nil {
			return nil, storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "生成成片上传地址失败", err)
		}
		result, err := a.store.readableURL(key)
		if err != nil {
			return nil, storageErr(channelv1.ErrorStage_STAGE_SUBMIT, "生成成片读取地址失败", err)
		}
		return &resultSink{putURL: put, headers: headers, resultURL: result, objectKey: key}, nil
	}
	return nil, inputErr("缺少结果 PUT 地址：videos[1]、config.json upload_url 或对象存储配置")
}

// materialize resolves what the platform is told on success. A local result is
// re-checked (and copied down from storage when that was the route), so a
// half-written file is a retryable poll failure instead of a bogus success.
func (a *assetBridge) materialize(ctx context.Context, sink *resultSink) (string, error) {
	if sink == nil {
		return "", nil
	}
	if sink.localDest == "" {
		return sink.resultURL, nil
	}
	if sink.objectKey != "" && statAsset(sink.localDest) != nil {
		if a == nil || a.store == nil {
			return "", assetPendingErr("成片仍在对象存储，渠道已失去存储配置")
		}
		if err := a.store.download(ctx, sink.objectKey, sink.localDest); err != nil {
			return "", storageErr(channelv1.ErrorStage_STAGE_QUERY, "回取成片失败", err)
		}
	}
	if err := statAsset(sink.localDest); err != nil {
		return "", assetPendingErr("上游报告完成但成片尚未落地: " + err.Error())
	}
	return fileURL(sink.localDest), nil
}

// release drops the per-task registrations once a task is terminal. It is safe
// to call repeatedly: a later poll of the same task only re-stats the result.
func (a *assetBridge) release(token string) {
	if a != nil && a.local != nil {
		a.local.release(token)
	}
}

func statAsset(p string) error {
	st, err := os.Stat(p)
	if err != nil {
		return fmt.Errorf("本机找不到文件")
	}
	if st.IsDir() {
		return fmt.Errorf("路径是目录，不是文件")
	}
	if st.Size() <= 0 {
		return fmt.Errorf("文件为空")
	}
	if st.Size() > maxAssetBytes {
		return fmt.Errorf("文件超过 1GiB 上限")
	}
	return nil
}

// withHeartbeat keeps the submit progress stream alive across a long transfer.
// The platform cancels a submit as stalled after 120s of silence, and a 1 GiB
// upload easily outlasts that.
func withHeartbeat(ctx context.Context, report func(stage, detail string), stage, detail string, fn func(context.Context) error) error {
	if report == nil {
		report = func(string, string) {}
	}
	report(stage, detail)
	done := make(chan struct{})
	defer close(done)
	go func() {
		ticker := time.NewTicker(assetHeartbeat)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				report(stage, detail+"（进行中）")
			}
		}
	}()
	return fn(ctx)
}

// --- built-in file server ---------------------------------------------------

// localAssetServer publishes exactly the files a live task registered: a GET
// endpoint for the source video and a PUT endpoint that streams the finished
// one to its destination path. Unregistered tokens 404, so there is no
// directory listing and no way to walk out of the registered set.
type localAssetServer struct {
	addr string

	mu      sync.RWMutex
	base    string
	started bool

	sources sync.Map // token → source path
	sinks   sync.Map // token → destination path
}

// assetServers keeps one listener per address for the process lifetime. A
// ReloadConfig that only edits the public base URL reuses the running listener;
// tasks in flight keep their URLs working.
var assetServers = struct {
	mu     sync.Mutex
	byAddr map[string]*localAssetServer
}{byAddr: map[string]*localAssetServer{}}

func newLocalAssetServer(cfg *runtime.Config) *localAssetServer {
	base := strings.TrimRight(strings.TrimSpace(cfg.String("asset_public_base_url", "")), "/")
	if base == "" {
		return nil
	}
	addr := strings.TrimSpace(cfg.String("asset_serve_addr", "127.0.0.1:19312"))

	assetServers.mu.Lock()
	defer assetServers.mu.Unlock()
	srv, ok := assetServers.byAddr[addr]
	if !ok {
		srv = &localAssetServer{addr: addr}
		assetServers.byAddr[addr] = srv
	}
	srv.mu.Lock()
	srv.base = base
	srv.mu.Unlock()
	return srv
}

func (s *localAssetServer) publicBase() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.base
}

// ensure starts the listener on first use, so a channel that never sees a local
// material never opens a port.
func (s *localAssetServer) ensure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.started {
		return nil
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", s.addr, err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc(assetSourcePath, s.serveSource)
	mux.HandleFunc(assetSinkPath, s.receiveSink)
	// No write timeout on purpose: a multi-hundred-megabyte transfer is normal
	// here, and the read header timeout is what protects against slow loris.
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 15 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	s.started = true
	return nil
}

func (s *localAssetServer) publishSource(token, path string) (string, error) {
	if err := s.ensure(); err != nil {
		return "", err
	}
	s.sources.Store(token, path)
	return s.publicBase() + assetSourcePath + token, nil
}

func (s *localAssetServer) publishSink(token, dest string) (string, error) {
	if err := s.ensure(); err != nil {
		return "", err
	}
	s.sinks.Store(token, dest)
	return s.publicBase() + assetSinkPath + token, nil
}

func (s *localAssetServer) release(token string) {
	s.sources.Delete(token)
	s.sinks.Delete(token)
}

func (s *localAssetServer) serveSource(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw, ok := s.sources.Load(assetTokenFromPath(r.URL.Path, assetSourcePath))
	if !ok {
		http.NotFound(w, r)
		return
	}
	// ServeFile handles Range requests, which the upstream fetcher uses.
	http.ServeFile(w, r, raw.(string))
}

func (s *localAssetServer) receiveSink(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut && r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	raw, ok := s.sinks.Load(assetTokenFromPath(r.URL.Path, assetSinkPath))
	if !ok {
		http.NotFound(w, r)
		return
	}
	dest := raw.(string)
	if err := writeStream(dest, r.Body); err != nil {
		http.Error(w, "write failed", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func assetTokenFromPath(urlPath, prefix string) string {
	return path.Base(strings.TrimPrefix(urlPath, prefix))
}

// writeStream lands the payload through a sibling temp file, so a caller that
// reads dest never sees a partially written video.
func writeStream(dest string, body io.Reader) error {
	tmp, err := os.CreateTemp(filepath.Dir(dest), "."+filepath.Base(dest)+".part-*")
	if err != nil {
		return err
	}
	name := tmp.Name()
	defer func() {
		_ = tmp.Close()
		_ = os.Remove(name)
	}()
	written, err := io.Copy(tmp, io.LimitReader(body, maxAssetBytes+1))
	if err != nil {
		return err
	}
	if written <= 0 || written > maxAssetBytes {
		return fmt.Errorf("成片大小不合理")
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(name, dest)
}
