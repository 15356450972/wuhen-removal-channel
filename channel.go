// Package main: channel.go is the business surface you implement. The method
// signatures are fixed by the runtime.Channel contract — do not change them. Fill in
// the TODOs; the framework (envelopes, idempotency, error normalization,
// heartbeat, graceful shutdown) is provided by the SDK and never appears here.
package main

import (
	"context"
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	channelv1 "github.com/mao-dispatch/channel-sdk/gen/channel/v1"
	"github.com/mao-dispatch/channel-sdk/runtime"
	wuhen "wuhen-sdk"
)

// inputSchema is this channel's capability input_schema, embedded from the file.
// It starts from the platform standard vocabulary and adds this channel's real
// constraints. Edit wuhen-removal.input.schema.json, not this line.
//
//go:embed wuhen-removal.input.schema.json
var inputSchema string

// SampleInput is used by `verify` for the submit→query smoke test.
// videos[0] is the source; videos[1] is the result PUT target.
var SampleInput = &channelv1.TaskInput{
	Prompt: "watermark_removal",
	Videos: []string{"https://example.invalid/in.mp4", "https://example.invalid/out.mp4"},
}

// Channel implements runtime.Channel for 无痕去水印.
type Channel struct {
	up     Upstream
	config *runtime.Config
	regs   sync.Map // registrationTaskID → registrationPending
}

// ListModels is the V1.6 model discovery source. Items contain exactly id/name.
// The ids are 无痕's own model values, forwarded verbatim on submit.
func (c *Channel) ListModels(context.Context) ([]*channelv1.Model, error) {
	return []*channelv1.Model{
		{ModelId: wuhen.ModelStd, ModelName: "无痕去水印 标准版"},
		{ModelId: wuhen.ModelPro, ModelName: "无痕去水印 高清版"},
	}, nil
}

const registrationPrefix = "wuhen-removal-reg-"

type accountCredential struct {
	APIKey string `json:"api_key"`
}

type registrationPending struct {
	Email    string
	Password string
}

func isWuhenAPIKey(s string) bool {
	s = strings.TrimSpace(s)
	return strings.HasPrefix(s, "sk_") || strings.HasPrefix(s, "sk-")
}

func (c *Channel) resolveAPIKey(password string) string {
	if isWuhenAPIKey(password) {
		return strings.TrimSpace(password)
	}
	if c.config != nil {
		return strings.TrimSpace(c.config.String("api_key", ""))
	}
	return ""
}

func registrationTaskID(idempotencyKey string) string {
	sum := sha256.Sum256([]byte("wuhen-removal/registration/v1\x00" + idempotencyKey))
	return registrationPrefix + hex.EncodeToString(sum[:16])
}

func (c *Channel) StartRegistration(_ context.Context, in *runtime.StartRegistrationInput) (*runtime.StartRegistrationResult, error) {
	if in == nil {
		return nil, registrationInputErr(channelv1.ErrorStage_STAGE_REGISTER_START, "缺少注册参数")
	}
	key := strings.TrimSpace(in.IdempotencyKey)
	if key == "" {
		key = strings.TrimSpace(in.PlatformRegistrationID)
	}
	if key == "" {
		return nil, registrationInputErr(channelv1.ErrorStage_STAGE_REGISTER_START, "缺少注册幂等键")
	}
	id := registrationTaskID(key)
	c.regs.Store(id, registrationPending{
		Email:    strings.TrimSpace(in.Email),
		Password: strings.TrimSpace(in.Password),
	})
	return &runtime.StartRegistrationResult{RegistrationTaskID: id, Status: channelv1.RegistrationStatus_REGISTRATION_RUNNING}, nil
}

func (c *Channel) QueryRegistration(_ context.Context, in *runtime.QueryRegistrationInput) (*runtime.QueryRegistrationResult, error) {
	if in == nil || strings.TrimSpace(in.RegistrationTaskID) == "" {
		return nil, registrationInputErr(channelv1.ErrorStage_STAGE_REGISTER_QUERY, "缺少 registration_task_id")
	}
	taskID := strings.TrimSpace(in.RegistrationTaskID)
	if !strings.HasPrefix(taskID, registrationPrefix) {
		return nil, registrationInputErr(channelv1.ErrorStage_STAGE_REGISTER_QUERY, "未知的 registration_task_id")
	}
	raw, ok := c.regs.Load(taskID)
	if !ok {
		return nil, registrationInputErr(channelv1.ErrorStage_STAGE_REGISTER_QUERY, "未知的 registration_task_id")
	}
	pending := raw.(registrationPending)
	if pending.Email == "" {
		return &runtime.QueryRegistrationResult{
			Status: channelv1.RegistrationStatus_REGISTRATION_FAILED,
			Err:    emailRequiredErr(),
		}, nil
	}
	apiKey := c.resolveAPIKey(pending.Password)
	if apiKey == "" {
		e := runtime.NewError(runtime.CodeInputInvalid, channelv1.ErrorCategory_INPUT_INVALID, false, "registration requires config.json api_key")
		e.Details = map[string]string{"reason": "API_KEY_REQUIRED"}
		e.Stage = channelv1.ErrorStage_STAGE_REGISTER_QUERY
		e.Source = "SDK"
		e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
		return &runtime.QueryRegistrationResult{
			Status: channelv1.RegistrationStatus_REGISTRATION_FAILED,
			Err:    e,
		}, nil
	}
	sum := sha256.Sum256([]byte("wuhen-removal/account/v1\x00" + taskID))
	accountID := "wuhen-" + hex.EncodeToString(sum[:8])
	credential, err := json.Marshal(accountCredential{APIKey: apiKey})
	if err != nil {
		return nil, fmt.Errorf("encode credential: %w", err)
	}
	return &runtime.QueryRegistrationResult{
		Status:            channelv1.RegistrationStatus_REGISTRATION_SUCCEEDED,
		ExternalAccountID: accountID,
		DisplayName:       pending.Email,
		Credential:        credential,
		// 平台 accounts.fingerprint 非空；无痕不用指纹，只回占位。
		Fingerprint: []byte("{}"),
	}, nil
}

func registrationInputErr(stage channelv1.ErrorStage, message string) *runtime.ChannelError {
	e := runtime.NewError(runtime.CodeInputInvalid, channelv1.ErrorCategory_INPUT_INVALID, false, message)
	e.Stage = stage
	e.Source = "SDK"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	return e
}

func emailRequiredErr() *runtime.ChannelError {
	e := runtime.NewError(runtime.CodeInputInvalid, channelv1.ErrorCategory_INPUT_INVALID, false, "registration requires a platform email")
	e.Stage = channelv1.ErrorStage_STAGE_REGISTER_QUERY
	e.Source = "SDK"
	e.AccountSignal = channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE
	e.Details = map[string]string{"reason": "EMAIL_REQUIRED"}
	return e
}

// New constructs the serving channel. Serve is ALWAYS live — there is no mock
// mode in production and no config switch that selects one. Until the operator
// completes config.json (base_url, ...), upstream calls fail with a structured
// CHANNEL_NOT_CONFIGURED error: the worker stays healthy for install, enable,
// and config editing, but it never fabricates results.
func New() *Channel {
	// config.json is this channel's freeform, operator-editable settings. The
	// platform's channel-SDK management UI reads/writes it, then calls ReloadConfig
	// so edits take effect without restarting the worker.
	//
	// MUST: read every plugin/business setting from this file via cfg.String/Bool.
	// Do not os.Getenv / os.LookupEnv for base_url, helpers, or flags.
	cfg, err := runtime.LoadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "config load (starting with empty config):", err)
		cfg = runtime.EmptyConfig()
	}
	return &Channel{up: selectUpstream(cfg), config: cfg}
}

// NewForVerify backs the offline `verify` subcommand with the built-in mock
// upstream. This is the ONLY entry point to the mock: `serve` never selects
// it, and no config.json value can.
func NewForVerify() *Channel {
	return &Channel{up: &mockUpstream{}, config: runtime.EmptyConfig()}
}

// ReloadConfig implements runtime.ConfigReloader: it re-reads config.json and
// rebuilds the upstream client so a platform-side edit (base_url, timeouts,
// ...) takes effect without restarting the worker. The platform calls this
// right after it saves the config file via the management UI.
func (c *Channel) ReloadConfig(context.Context) error {
	if c.config == nil {
		c.config = runtime.EmptyConfig()
	}
	if err := c.config.Reload(); err != nil {
		return err
	}
	c.up = selectUpstream(c.config)
	return nil
}

// selectUpstream builds the real provider client from config.json ONLY.
// Never read os.Getenv / os.LookupEnv for this decision.
//
// There is deliberately no mock branch here. An incomplete config yields
// notConfiguredUpstream, whose calls return a structured error naming the
// missing setting — a live channel must fail loudly, because silently serving
// fake results poisons platform verification and billing.
func selectUpstream(cfg *runtime.Config) Upstream {
	if cfg == nil {
		return &notConfiguredUpstream{missing: "config.json"}
	}
	if strings.TrimSpace(cfg.String("base_url", "")) == "" {
		return &notConfiguredUpstream{missing: "base_url"}
	}
	return newRealUpstream(cfg)
}

// NewFailingClone implements runtime.VerifyFailureProbe: it returns this channel
// backed by an upstream that reports a terminal task failure, so `verify` can
// confirm Query surfaces it as FAILED+error rather than as a Go error. Keep this
// wired as you replace the mock — it is what keeps the failure path honest.
func (c *Channel) NewFailingClone() (runtime.Channel, error) {
	return &Channel{up: &failingUpstream{}}, nil
}

// Capabilities is compatibility metadata, one entry per upstream model.
// SupportsCancel stays false: 无痕 exposes no cancel endpoint.
func (c *Channel) Capabilities(ctx context.Context) ([]*channelv1.Capability, error) {
	capability := func(modelKey, display string) *channelv1.Capability {
		return &channelv1.Capability{
			ModelKey:          modelKey,
			DisplayName:       display,
			TaskType:          "video.watermark_removal",
			CapabilityVersion: "v1",
			InputSchema:       inputSchema,
			SupportedFeatures: []string{"all_area_removal", "rect_removal"},
			Limits: map[string]string{
				"videos": "source + optional upload target",
				"images": "original url, no removal",
			},
			SupportsCancel: false,
		}
	}
	return []*channelv1.Capability{
		capability(wuhen.ModelStd, "无痕去水印 标准版"),
		capability(wuhen.ModelPro, "无痕去水印 高清版"),
	}, nil
}

// Submit creates a task upstream. The framework has already handled idempotency:
// this is only called for a genuinely new key. Return the provider's own task id
// as soon as it is bound — never wait for generation here (results belong to
// Query; the platform cuts submit at CHANNEL_SUBMIT_TIMEOUT, default 16m).
func (c *Channel) Submit(ctx context.Context, in *runtime.SubmitInput) (*runtime.SubmitResult, error) {
	// in.Report publishes submit progress the platform polls while this call is
	// in flight (never nil; no secrets in stage/detail; either don't report at
	// all, or keep ≤120s between events while the submit is active).
	input := in.Input
	if input == nil {
		input = &channelv1.TaskInput{}
	}
	plan, err := planRemoval(input)
	if err != nil {
		return nil, err
	}
	if plan.skipUpstream {
		// 图片不调用无痕。平台若给了结果 PUT 地址，要把原图字节写进去，
		// 否则落地校验会报对象不存在。没有落点时才直接回原链接。
		urls, err := c.finishImagePassthrough(ctx, plan, in.IdempotencyKey, in.Report)
		if err != nil {
			return nil, err
		}
		id, err := encodeImagePassthrough(urls)
		if err != nil {
			return nil, err
		}
		return &runtime.SubmitResult{ProviderTaskID: id, Status: channelv1.TaskStatus_QUEUED}, nil
	}
	in.Report("UPSTREAM_CALL", "正在调用上游创建任务")
	providerID, err := c.up.Create(ctx, input, in)
	if err != nil {
		return nil, err // return a *runtime.ChannelError for structured upstream failures
	}
	return &runtime.SubmitResult{ProviderTaskID: providerID, Status: channelv1.TaskStatus_QUEUED}, nil
}

// Query polls the upstream task and maps its status into the platform status.
func (c *Channel) Query(ctx context.Context, in *runtime.QueryInput) (*runtime.QueryResult, error) {
	// A returned error means the poll itself failed and the task's fate is still
	// unknown — the scheduler may retry. A task that failed upstream is NOT an
	// error here: it comes back as a FAILED status carrying pr.Err (see below).
	if in != nil {
		if urls, matched, decErr := lookupImagePassthrough(in.ProviderTaskID); matched {
			if decErr != nil {
				return &runtime.QueryResult{
					Status:         channelv1.TaskStatus_FAILED,
					ProviderStatus: "failed",
					Err:            decErr,
				}, nil
			}
			return &runtime.QueryResult{
				Status:         channelv1.TaskStatus_SUCCEEDED,
				ProviderStatus: "succeeded",
				Progress:       100,
				Result: &runtime.TaskPayload{
					ID:       in.ProviderTaskID,
					URL:      urls,
					Metadata: map[string]any{"passthrough": "image"},
				},
			}, nil
		}
	}
	pr, err := c.up.Poll(ctx, in.ProviderTaskID, in.Account)
	if err != nil {
		return nil, err
	}

	status := mapStatus(pr.RawStatus)
	res := &runtime.QueryResult{
		Status:         status,
		ProviderStatus: pr.RawStatus,
		Progress:       pr.Progress,
		Result:         pr.Payload,
	}

	// Terminal upstream failure: report FAILED *with* the reason attached, never
	// as a Go error — returning an error would land a dead task in a retry loop.
	if status == channelv1.TaskStatus_FAILED {
		res.Err = pr.Err
		if res.Err == nil {
			// The provider said failed but gave us nothing to explain it. Still
			// terminal, so synthesize a non-retryable error rather than leave it blank.
			res.Err = runtime.NewError(runtime.CodeErrorUnknown,
				channelv1.ErrorCategory_ERROR_UNKNOWN, false,
				"upstream reported status "+pr.RawStatus+" with no error detail")
		}
	}
	return res, nil
}

// mapStatus translates 无痕's status string into the platform TaskStatus. The
// cancelled cases are checked before wuhen.StatusFailed, which folds them in
// with the failures.
func mapStatus(upstream string) channelv1.TaskStatus {
	s := strings.ToLower(strings.TrimSpace(upstream))
	switch {
	case s == "queued" || s == "pending":
		return channelv1.TaskStatus_QUEUED
	case s == "processing" || s == "running" || s == "in_progress":
		return channelv1.TaskStatus_RUNNING
	case s == "cancelled" || s == "canceled":
		return channelv1.TaskStatus_CANCELLED
	case wuhen.StatusDone(s):
		return channelv1.TaskStatus_SUCCEEDED
	case wuhen.StatusFailed(s):
		return channelv1.TaskStatus_FAILED
	default:
		return channelv1.TaskStatus_TASK_STATUS_UNKNOWN
	}
}

// CheckTask is where an asset-library task gets its cheap pre-flight: the
// platform calls it before leasing an account, so an unreadable local material
// or a channel with no exit for one costs nothing instead of burning a lease
// and an upstream submit.
func (c *Channel) CheckTask(_ context.Context, in *runtime.CheckTaskInput) (*runtime.CheckTaskResult, error) {
	if in == nil || in.Input == nil {
		return &runtime.CheckTaskResult{Eligible: true}, nil
	}
	plan, planErr := planRemoval(in.Input)
	if planErr != nil {
		if ce, ok := planErr.(*runtime.ChannelError); ok && strings.Contains(ce.Message, "超过") {
			return &runtime.CheckTaskResult{Eligible: false, Reason: ce.Message}, nil
		}
		return &runtime.CheckTaskResult{Eligible: true}, nil
	}
	if plan.skipUpstream || !isFileURL(plan.video) {
		return &runtime.CheckTaskResult{Eligible: true}, nil
	}
	local, err := localPathFromURL(plan.video)
	if err != nil {
		return &runtime.CheckTaskResult{Eligible: false, Reason: "videos[0] " + err.Error()}, nil
	}
	if err := statAsset(local); err != nil {
		return &runtime.CheckTaskResult{Eligible: false, Reason: "videos[0] " + err.Error()}, nil
	}
	if ru, ok := c.up.(*realUpstream); ok && !ru.assets.available() {
		return &runtime.CheckTaskResult{
			Eligible: false,
			Reason:   "本渠道未配置本地素材出口，需要 config.json 补 " + ru.assets.missing(),
		}, nil
	}
	return &runtime.CheckTaskResult{Eligible: true}, nil
}

func (c *Channel) CheckAccount(ctx context.Context, in *runtime.CheckAccountInput) (*runtime.CheckAccountResult, error) {
	if ru, ok := c.up.(*realUpstream); ok {
		client, err := ru.clientFor(in.Account, channelv1.ErrorStage_STAGE_CHECK)
		if err != nil {
			return nil, err
		}
		info, err := client.Me(ctx)
		if err != nil {
			return nil, upstreamErr(channelv1.ErrorStage_STAGE_CHECK, err)
		}
		pts := int64(info.Balance)
		return &runtime.CheckAccountResult{
			Points: &pts,
			Signal: channelv1.AccountSignal_ACCOUNT_SIGNAL_NONE,
			Detail: "wuhen balance",
		}, nil
	}
	return &runtime.CheckAccountResult{Points: runtime.Int64(-1), Detail: "no points on verify mock"}, nil
}
