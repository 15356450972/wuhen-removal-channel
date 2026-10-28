package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path"
	"strings"
	"time"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
)

const (
	imagePassthroughPrefix = "wuhen-image:"
	maxPassthroughImages   = 16
	maxPassthroughIDBytes  = 1 << 20
)

// imageExtensions are path suffixes that mean "do not send this to video removal".
// A URL with no suffix stays a video: CDN links often hide the real type, and
// guessing those as images would skip a removal the caller asked for.
var imageExtensions = map[string]struct{}{
	".jpg": {}, ".jpeg": {}, ".png": {}, ".webp": {}, ".gif": {},
	".bmp": {}, ".heic": {}, ".heif": {}, ".avif": {}, ".tif": {}, ".tiff": {},
	".svg": {},
}

// removalPlan is how one task splits into a video the upstream must process
// and image links that are echoed unchanged.
type removalPlan struct {
	video        string
	upload       string
	images       []string
	skipUpstream bool
}

// planRemoval splits a task. Images never go upstream. videos[0] is the video
// to clean, unless its path is an image — then it is echoed like images[].
// videos[1] is kept for images too: it is the landing PUT the platform checks.
func planRemoval(input *channelv1.TaskInput) (removalPlan, error) {
	if input == nil {
		input = &channelv1.TaskInput{}
	}
	images := uniqueRefs(input.GetImages())
	var src, upload string
	videos := input.GetVideos()
	if len(videos) > 0 {
		src = strings.TrimSpace(videos[0])
	}
	if len(videos) >= 2 {
		upload = strings.TrimSpace(videos[1])
	}
	if src != "" && isImageRef(src) {
		images = uniqueRefs(append([]string{src}, images...))
		src = ""
		// videos[1] stays. The platform presigns that PUT and later HEADs the
		// object; dropping it makes the landing check fail with 对象不存在.
	}
	if len(images) > maxPassthroughImages {
		return removalPlan{}, inputErr(fmt.Sprintf("images 超过 %d 张", maxPassthroughImages))
	}
	if src == "" {
		if len(images) == 0 {
			return removalPlan{}, inputErr("缺少 videos[0]：待处理视频地址")
		}
		return removalPlan{upload: upload, images: images, skipUpstream: true}, nil
	}
	return removalPlan{video: src, upload: upload, images: images}, nil
}

func uniqueRefs(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, raw := range in {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		if _, ok := seen[raw]; ok {
			continue
		}
		seen[raw] = struct{}{}
		out = append(out, raw)
	}
	return out
}

func isImageRef(raw string) bool {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return false
	}
	if strings.HasPrefix(strings.ToLower(raw), "data:image/") {
		return true
	}
	pathPart := raw
	if parsed, err := url.Parse(raw); err == nil && parsed.Scheme != "" && parsed.Path != "" {
		pathPart = parsed.Path
	}
	_, ok := imageExtensions[strings.ToLower(path.Ext(pathPart))]
	return ok
}

// encodeImagePassthrough packs the original links into the provider task id.
// Query only receives that id, including after a worker restart, so the links
// have to travel with it.
func encodeImagePassthrough(urls []string) (string, error) {
	raw, err := json.Marshal(urls)
	if err != nil {
		return "", inputErr("图片地址无法编码")
	}
	id := imagePassthroughPrefix + base64.RawURLEncoding.EncodeToString(raw)
	if len(id) > maxPassthroughIDBytes {
		return "", inputErr("图片地址过长，无法直接回传")
	}
	return id, nil
}

// lookupImagePassthrough reports whether id is an image echo task.
// A matched id with a non-nil error is corrupt and must not be retried.
func lookupImagePassthrough(id string) (urls []string, matched bool, err *runtime.ChannelError) {
	rest, found := strings.CutPrefix(strings.TrimSpace(id), imagePassthroughPrefix)
	if !found {
		return nil, false, nil
	}
	raw, decErr := base64.RawURLEncoding.DecodeString(rest)
	if decErr != nil {
		return nil, true, imagePassthroughDecodeErr()
	}
	if jsonErr := json.Unmarshal(raw, &urls); jsonErr != nil || len(urls) == 0 {
		return nil, true, imagePassthroughDecodeErr()
	}
	for _, item := range urls {
		if strings.TrimSpace(item) == "" {
			return nil, true, imagePassthroughDecodeErr()
		}
	}
	return urls, true, nil
}

func imagePassthroughDecodeErr() *runtime.ChannelError {
	e := inputErr("图片回传任务无法解析")
	e.Stage = channelv1.ErrorStage_STAGE_QUERY
	return e
}

// finishImagePassthrough copies the original image onto the platform's result
// PUT when one was given, and returns that landing URL. With no landing slot
// it returns the original links unchanged.
func (c *Channel) finishImagePassthrough(ctx context.Context, plan removalPlan, idempotencyKey string, report func(string, string)) ([]string, error) {
	if report == nil {
		report = func(string, string) {}
	}
	sink, err := c.imageSink(plan, idempotencyKey)
	if err != nil {
		return nil, err
	}
	if sink == nil {
		report("IMAGE_PASSTHROUGH", "图片不去水印，直接返回原链接")
		log.Printf("wuhen-removal: image passthrough sources=%d landing=false", len(plan.images))
		return plan.images, nil
	}
	report("IMAGE_PASSTHROUGH", "图片不去水印，正在把原图写到结果地址")
	if err := landOriginalImage(ctx, report, plan.images[0], sink); err != nil {
		return nil, err
	}
	if strings.TrimSpace(sink.resultURL) == "" {
		return plan.images, nil
	}
	return []string{sink.resultURL}, nil
}

// imageSink is the result PUT for an image task. A nil sink means the caller
// did not name a landing slot and this channel has no object storage, so the
// original link is the whole result.
func (c *Channel) imageSink(plan removalPlan, idempotencyKey string) (*resultSink, error) {
	if len(plan.images) == 0 {
		return nil, inputErr("缺少可回传的图片")
	}
	ru, _ := c.up.(*realUpstream)
	headers := imageContentHeader(plan.images[0])
	configUpload := ""
	bridge := &assetBridge{}
	if ru != nil {
		if strings.TrimSpace(ru.uploadHeaders) != "" {
			headers = strings.TrimSpace(ru.uploadHeaders)
		}
		configUpload = ru.uploadURL
		if ru.assets != nil {
			bridge = ru.assets
		}
	}
	if strings.TrimSpace(plan.upload) == "" && strings.TrimSpace(configUpload) == "" && bridge.store == nil {
		return nil, nil
	}
	return bridge.sink(assetToken(idempotencyKey), plan.upload, configUpload, headers)
}

func imageContentHeader(raw string) string {
	ext := strings.ToLower(path.Ext(imagePath(raw)))
	contentType := map[string]string{
		".png":  "image/png",
		".jpg":  "image/jpeg",
		".jpeg": "image/jpeg",
		".webp": "image/webp",
		".gif":  "image/gif",
		".bmp":  "image/bmp",
		".svg":  "image/svg+xml",
		".avif": "image/avif",
		".tif":  "image/tiff",
		".tiff": "image/tiff",
		".heic": "image/heic",
		".heif": "image/heif",
	}[ext]
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	encoded, _ := json.Marshal(map[string]string{"Content-Type": contentType})
	return string(encoded)
}

func imagePath(raw string) string {
	raw = strings.TrimSpace(raw)
	if parsed, err := url.Parse(raw); err == nil && parsed.Scheme != "" && parsed.Path != "" {
		return parsed.Path
	}
	return raw
}

func landOriginalImage(ctx context.Context, report func(string, string), source string, sink *resultSink) error {
	return withHeartbeat(ctx, report, "IMAGE_PASSTHROUGH", "图片不去水印，正在把原图写到结果地址", func(ctx context.Context) error {
		tmp, n, err := spoolImage(ctx, source)
		if err != nil {
			return err
		}
		defer os.Remove(tmp)
		if err := putImageFile(ctx, sink, tmp, n); err != nil {
			return err
		}
		log.Printf("wuhen-removal: image copied bytes=%d", n)
		return nil
	})
}

func spoolImage(ctx context.Context, source string) (string, int64, error) {
	body, err := openImageSource(ctx, source)
	if err != nil {
		return "", 0, err
	}
	defer body.Close()
	tmp, err := os.CreateTemp("", "wuhen-image-*")
	if err != nil {
		return "", 0, imageLandErr(true, "原图暂存失败")
	}
	path := tmp.Name()
	n, copyErr := io.Copy(tmp, io.LimitReader(body, maxAssetBytes+1))
	closeErr := tmp.Close()
	if copyErr != nil {
		os.Remove(path)
		return "", 0, imageLandErr(true, "读取原图失败")
	}
	if closeErr != nil {
		os.Remove(path)
		return "", 0, imageLandErr(true, "原图暂存失败")
	}
	if n == 0 {
		os.Remove(path)
		return "", 0, imageLandErr(false, "原图是空文件")
	}
	if n > maxAssetBytes {
		os.Remove(path)
		return "", 0, imageLandErr(false, "原图超过 1GiB 上限")
	}
	return path, n, nil
}

func openImageSource(ctx context.Context, source string) (io.ReadCloser, error) {
	source = strings.TrimSpace(source)
	switch {
	case isFileURL(source):
		local, err := localPathFromURL(source)
		if err != nil {
			return nil, inputErr("图片 " + err.Error())
		}
		f, err := os.Open(local)
		if err != nil {
			return nil, imageLandErr(false, "本机找不到原图")
		}
		return f, nil
	case strings.HasPrefix(strings.ToLower(source), "http://"), strings.HasPrefix(strings.ToLower(source), "https://"):
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, source, nil)
		if err != nil {
			return nil, inputErr("图片地址无法读取")
		}
		resp, err := imageHTTPClient.Do(req)
		if err != nil {
			return nil, imageLandErr(true, "读取原图失败")
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			resp.Body.Close()
			return nil, imageLandErr(resp.StatusCode >= 500, fmt.Sprintf("读取原图失败: HTTP %d", resp.StatusCode))
		}
		return resp.Body, nil
	default:
		return nil, inputErr("图片地址无法读取")
	}
}

func putImageFile(ctx context.Context, sink *resultSink, path string, n int64) error {
	f, err := os.Open(path)
	if err != nil {
		return imageLandErr(true, "原图暂存失败")
	}
	defer f.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, sink.putURL, f)
	if err != nil {
		return inputErr("结果地址无法写入")
	}
	req.ContentLength = n
	applyHeaderJSON(req.Header, sink.headers)
	resp, err := imageHTTPClient.Do(req)
	if err != nil {
		return imageLandErr(true, "原图写入结果地址失败")
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return imageLandErr(resp.StatusCode >= 500, fmt.Sprintf("原图写入结果地址失败: HTTP %d", resp.StatusCode))
	}
	return nil
}

func applyHeaderJSON(h http.Header, raw string) {
	var parsed map[string]string
	if err := json.Unmarshal([]byte(strings.TrimSpace(raw)), &parsed); err != nil {
		h.Set("Content-Type", "application/octet-stream")
		return
	}
	for key, value := range parsed {
		if strings.TrimSpace(key) != "" {
			h.Set(key, value)
		}
	}
	if h.Get("Content-Type") == "" {
		h.Set("Content-Type", "application/octet-stream")
	}
}

func imageLandErr(retryable bool, msg string) *runtime.ChannelError {
	code := runtime.CodeUpstreamError
	category := channelv1.ErrorCategory_ERROR_UNKNOWN
	if retryable {
		code = runtime.CodeChannelUnavailable
		category = channelv1.ErrorCategory_CHANNEL_UNAVAILABLE
	}
	e := runtime.NewError(code, category, retryable, msg)
	e.Stage = channelv1.ErrorStage_STAGE_SUBMIT
	e.Source = "SDK"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

var imageHTTPClient = &http.Client{Timeout: 10 * time.Minute}
