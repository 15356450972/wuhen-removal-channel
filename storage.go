// Package main: storage.go is the object-storage exit for local material. It
// speaks the S3-compatible REST API with SigV4 directly — TOS, MinIO and S3
// itself all answer it — so the packaged plugin stays a single static binary
// with no vendored cloud SDK. It does three things and nothing more: upload a
// local file, presign a PUT for 无痕 to write the result into, and read an
// object back down.
package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
)

const (
	sigV4Algorithm  = "AWS4-HMAC-SHA256"
	sigV4Service    = "s3"
	unsignedPayload = "UNSIGNED-PAYLOAD"
	emptyPayloadSHA = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

// objectStore is the configured bucket. A nil *objectStore means the operator
// has not filled the storage_* keys, which every caller treats as "this exit
// does not exist" rather than as an error on its own.
type objectStore struct {
	scheme     string
	host       string
	region     string
	bucket     string
	accessKey  string
	secretKey  string
	prefix     string
	publicBase string
	pathStyle  bool
	ttl        time.Duration
	http       *http.Client
}

// newObjectStore builds the client from config.json. Storage settings are
// operator configuration, not account credentials, so they belong here rather
// than in the account credential the platform hands down per task.
func newObjectStore(cfg *runtime.Config) *objectStore {
	endpoint := strings.TrimSpace(cfg.String("storage_endpoint", ""))
	region := strings.TrimSpace(cfg.String("storage_region", ""))
	bucket := strings.TrimSpace(cfg.String("storage_bucket", ""))
	accessKey := strings.TrimSpace(cfg.String("storage_access_key", ""))
	secretKey := strings.TrimSpace(cfg.String("storage_secret_key", ""))
	if endpoint == "" || region == "" || bucket == "" || accessKey == "" || secretKey == "" {
		return nil
	}
	if !strings.Contains(endpoint, "://") {
		endpoint = "https://" + endpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		return nil
	}
	timeout := time.Duration(cfg.Int("storage_timeout_seconds", 900)) * time.Second
	if timeout <= 0 {
		timeout = 900 * time.Second
	}
	ttl := time.Duration(cfg.Int("storage_url_ttl_seconds", 172800)) * time.Second
	if ttl < time.Minute {
		ttl = time.Minute
	}
	if ttl > 7*24*time.Hour {
		ttl = 7 * 24 * time.Hour
	}
	prefix := strings.Trim(strings.TrimSpace(cfg.String("storage_prefix", "")), "/")
	if prefix == "" {
		prefix = "wuhen"
	}
	return &objectStore{
		scheme:     parsed.Scheme,
		host:       parsed.Host,
		region:     region,
		bucket:     bucket,
		accessKey:  accessKey,
		secretKey:  secretKey,
		prefix:     prefix,
		publicBase: strings.TrimRight(strings.TrimSpace(cfg.String("storage_public_base_url", "")), "/"),
		pathStyle:  cfg.Bool("storage_path_style", false),
		ttl:        ttl,
		http:       &http.Client{Timeout: timeout},
	}
}

// key lays material out as <prefix>/<in|out>/<date>/<token><ext>, so an
// operator can expire the two sides on different lifecycle rules.
func (s *objectStore) key(kind, token, ext string) string {
	ext = strings.ToLower(strings.TrimSpace(ext))
	if ext == "" || len(ext) > 8 || strings.ContainsAny(ext, "/\\") {
		ext = ".mp4"
	}
	return path.Join(s.prefix, kind, time.Now().UTC().Format("2006/01/02"), token+ext)
}

// put streams a local file into the bucket with a signed payload hash.
func (s *objectStore) put(ctx context.Context, key, localPath string) error {
	f, err := os.Open(localPath)
	if err != nil {
		return err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return err
	}
	sum, err := sha256Reader(f)
	if err != nil {
		return err
	}
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	endpoint, canonicalPath, host := s.endpoint(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, endpoint, f)
	if err != nil {
		return err
	}
	req.ContentLength = st.Size()
	req.Header.Set("Content-Type", contentTypeFor(key))
	s.sign(req, canonicalPath, host, sum, time.Now().UTC())
	return s.send(req, nil)
}

// download copies an object to dest, used when the result had to land in the
// bucket first because nothing outside could reach this worker.
func (s *objectStore) download(ctx context.Context, key, dest string) error {
	endpoint, canonicalPath, host := s.endpoint(key)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return err
	}
	s.sign(req, canonicalPath, host, emptyPayloadSHA, time.Now().UTC())
	return s.send(req, func(body io.Reader) error { return writeStream(dest, body) })
}

func (s *objectStore) send(req *http.Request, consume func(io.Reader) error) error {
	resp, err := s.http.Do(req)
	if err != nil {
		return fmt.Errorf("%s", scrubStorage(err.Error()))
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		detail, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return fmt.Errorf("对象存储 HTTP %d: %s", resp.StatusCode, scrubStorage(string(detail)))
	}
	if consume == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil
	}
	return consume(resp.Body)
}

// presign builds a query-signed URL. Only host is signed, so 无痕 may send
// whatever Content-Type it likes on the PUT without breaking the signature.
func (s *objectStore) presign(method, key string) (string, error) {
	endpoint, canonicalPath, host := s.endpoint(key)
	now := time.Now().UTC()
	stamp := now.Format("20060102")
	scope := stamp + "/" + s.region + "/" + sigV4Service + "/aws4_request"

	q := url.Values{}
	q.Set("X-Amz-Algorithm", sigV4Algorithm)
	q.Set("X-Amz-Credential", s.accessKey+"/"+scope)
	q.Set("X-Amz-Date", now.Format("20060102T150405Z"))
	q.Set("X-Amz-Expires", strconv.FormatInt(int64(s.ttl/time.Second), 10))
	q.Set("X-Amz-SignedHeaders", "host")
	canonicalQuery := q.Encode()

	canonicalRequest := strings.Join([]string{
		method,
		canonicalPath,
		canonicalQuery,
		"host:" + host + "\n",
		"host",
		unsignedPayload,
	}, "\n")
	signature := s.signature(stamp, now, scope, canonicalRequest)
	return endpoint + "?" + canonicalQuery + "&X-Amz-Signature=" + signature, nil
}

// readableURL is what the platform is told about a finished object: the public
// domain when the operator fronted the bucket with one, otherwise a presigned
// GET that expires with storage_url_ttl_seconds.
func (s *objectStore) readableURL(key string) (string, error) {
	if s.publicBase != "" {
		return s.publicBase + "/" + encodeObjectPath(key), nil
	}
	return s.presign(http.MethodGet, key)
}

// endpoint returns the request URL plus the exact canonical path that URL will
// be sent with, so the signature can never disagree with the request line.
func (s *objectStore) endpoint(key string) (raw, canonicalPath, host string) {
	encoded := encodeObjectPath(key)
	host = s.host
	canonicalPath = "/" + encoded
	if s.pathStyle {
		canonicalPath = "/" + s.bucket + "/" + encoded
	} else {
		host = s.bucket + "." + s.host
	}
	return s.scheme + "://" + host + canonicalPath, canonicalPath, host
}

func (s *objectStore) sign(req *http.Request, canonicalPath, host, payloadHash string, now time.Time) {
	amzDate := now.Format("20060102T150405Z")
	stamp := now.Format("20060102")
	scope := stamp + "/" + s.region + "/" + sigV4Service + "/aws4_request"
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	const signedHeaders = "host;x-amz-content-sha256;x-amz-date"
	canonicalHeaders := "host:" + host + "\n" +
		"x-amz-content-sha256:" + payloadHash + "\n" +
		"x-amz-date:" + amzDate + "\n"
	canonicalRequest := strings.Join([]string{
		req.Method,
		canonicalPath,
		req.URL.RawQuery,
		canonicalHeaders,
		signedHeaders,
		payloadHash,
	}, "\n")
	signature := s.signature(stamp, now, scope, canonicalRequest)
	req.Header.Set("Authorization", sigV4Algorithm+
		" Credential="+s.accessKey+"/"+scope+
		", SignedHeaders="+signedHeaders+
		", Signature="+signature)
}

func (s *objectStore) signature(stamp string, now time.Time, scope, canonicalRequest string) string {
	stringToSign := strings.Join([]string{
		sigV4Algorithm,
		now.Format("20060102T150405Z"),
		scope,
		sha256Hex([]byte(canonicalRequest)),
	}, "\n")
	key := hmacSHA256([]byte("AWS4"+s.secretKey), stamp)
	key = hmacSHA256(key, s.region)
	key = hmacSHA256(key, sigV4Service)
	key = hmacSHA256(key, "aws4_request")
	return hex.EncodeToString(hmacSHA256(key, stringToSign))
}

func hmacSHA256(key []byte, data string) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(data))
	return mac.Sum(nil)
}

func sha256Hex(b []byte) string {
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}

func sha256Reader(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// encodeObjectPath percent-encodes every character SigV4 does not treat as
// unreserved, per path segment. Go's own path escaping leaves sub-delimiters
// such as ':' and '=' alone, which would desync the signature from the URL.
func encodeObjectPath(key string) string {
	segments := strings.Split(strings.TrimPrefix(key, "/"), "/")
	for i, segment := range segments {
		var b strings.Builder
		for j := 0; j < len(segment); j++ {
			c := segment[j]
			switch {
			case c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z', c >= '0' && c <= '9',
				c == '-', c == '.', c == '_', c == '~':
				b.WriteByte(c)
			default:
				fmt.Fprintf(&b, "%%%02X", c)
			}
		}
		segments[i] = b.String()
	}
	return strings.Join(segments, "/")
}

func contentTypeFor(key string) string {
	if t := mime.TypeByExtension(path.Ext(key)); t != "" {
		return t
	}
	return "application/octet-stream"
}

var storageSecretQuery = regexp.MustCompile(`(?i)(X-Amz-Signature|X-Amz-Credential|X-Amz-Security-Token)=[^&"\s]*`)

// scrubStorage keeps signatures and access keys out of anything we return to
// the platform, which stores and displays error text verbatim.
func scrubStorage(msg string) string {
	return strings.TrimSpace(storageSecretQuery.ReplaceAllString(msg, "$1=***"))
}

// storageErr classifies a transfer failure. A rejected signature or a missing
// bucket is a configuration fault that retrying cannot fix; everything else is
// transport trouble worth another attempt.
func storageErr(stage channelv1.ErrorStage, what string, err error) *runtime.ChannelError {
	msg := what + ": " + scrubStorage(err.Error())
	code, category, retryable := int32(runtime.CodeChannelUnavailable), channelv1.ErrorCategory_CHANNEL_UNAVAILABLE, true
	switch {
	case strings.Contains(msg, "HTTP 403"), strings.Contains(msg, "SignatureDoesNotMatch"),
		strings.Contains(msg, "InvalidAccessKeyId"), strings.Contains(msg, "AccessDenied"),
		strings.Contains(msg, "NoSuchBucket"), strings.Contains(msg, "HTTP 404"):
		code, category, retryable = runtime.CodeChannelNotConfigured, channelv1.ErrorCategory_CHANNEL_UNAVAILABLE, false
	}
	e := runtime.NewError(code, category, retryable, msg)
	e.Stage = stage
	e.Source = "SDK"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

// assetPendingErr says the upstream is done but the bytes are not here yet.
// It is a poll-side failure, not a task failure: the next poll re-checks.
func assetPendingErr(msg string) *runtime.ChannelError {
	e := runtime.NewError(runtime.CodeChannelUnavailable, channelv1.ErrorCategory_CHANNEL_UNAVAILABLE, true, msg)
	e.Stage = channelv1.ErrorStage_STAGE_QUERY
	e.Source = "SDK"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}
