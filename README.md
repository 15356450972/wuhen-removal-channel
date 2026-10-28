# 无痕去水印 channel plugin

A standalone channel SDK plugin for **无痕去水印** (WuhenAI), task type `video.watermark_removal`.
It compiles to a self-contained gRPC worker binary and depends only on the shared
`channel-sdk` contract — never on the scheduler.

## Layout

| File | What it is | You edit it? |
|------|------------|--------------|
| `main.go` | serve/verify wiring (frozen) | no |
| `channel.go` | ListModels, registration, Submit/Query — implemented | only for behaviour changes |
| `upstream.go` | 无痕 `/video_removal` + `/status` calls — implemented | only for behaviour changes |
| `assets.go` | 资源库 `file://` 素材出口：对象存储优先，内置文件服务兜底 | only for behaviour changes |
| `storage.go` | S3 兼容对象存储（SigV4，纯标准库） | only for behaviour changes |
| `wuhen-removal.input.schema.json` | this channel's input constraints | when the upstream contract moves |
| `manifest.json` | plugin package metadata | yes — bump version on every release |
| `config.json` | operator-editable runtime settings | yes — `base_url` / timeout |

The protocol client lives outside this repo in `leonardo-go/third_party/wuhen_sdk`
(module `wuhen-sdk`), wired in through a `replace` in `go.mod` and shared with
the leonardo-go server so token exchange and status parsing exist only once.

## 本渠道约定

无痕没有谷歌/微软登录。邮箱池用现成的谷歌或微软地址即可，密码不用改成 Key。
`QueryRegistration` 空邮箱报 `EMAIL_REQUIRED`。API Key 优先用 password 里 `sk_` 开头的值，否则用 `config.json.api_key`，写入 credential。Submit / Query 只用 credential。fingerprint 只回 `{}` 占位。

任务入参是协议冻结的七字段 `TaskInput`，不是自由 JSON。只对视频去水印。
`videos[0]` 是待处理片，`videos[1]` 是可选的结果落点。没给 `videos[1]` 时用
`config.json.upload_url`，再没有就用对象存储自动开一个。成片由上游写过去。
回给平台的视频 `TaskPayload.URL` 去掉签名 query。

`images` 不提交无痕，也不占用去水印额度。`videos[0]` 的路径若是图片后缀（jpg/png/webp 等）
或 `data:image/`，同样不提交无痕。没有后缀的地址仍当视频。
平台如果给了 `videos[1]`（或 `upload_url` / 对象存储）作为结果落点，渠道把原图字节写到那个地址，
Query 回落点链接并去掉签名，这样平台的落地校验能看到对象。没有落点时才直接回原链接，query 保留。
同一任务里既有视频又有图片时，视频照常去水印，成功结果先放成片地址，再附上图片原链接。

| `config.json` 键 | 含义 | 缺省 |
|---|---|---|
| `base_url` | 无痕 API 根地址 | `https://api.wuhenai.com/v2` |
| `api_key` | 无痕 API Key。邮箱池是普通谷歌/微软密码时用这个 | 空 |
| `upload_url` | 任务没带 `videos[1]` 时的结果 PUT 地址 | 空 |
| `upload_headers` | 配合 `upload_url` 的请求头 JSON | `{"Content-Type":"application/octet-stream"}` |
| `http_timeout_seconds` | 单次 HTTP 超时 | 60 |

## 本机 `file://` 素材（平台资源库）

平台资源库开关**打开**（默认）时，素材先由资源库下载去重，交到渠道手里的
`videos[]` 是本机 `file://` 路径；关闭时任务带调用方原始链接直连。两种都支持，
渠道按 scheme 自己分流。

无痕没有上传接口——`POST /video_removal` 只有 `video_url`（上游自己 GET）和
`upload_url`（上游自己 PUT）——所以本地路径必须先变成公网地址。两条出口，
按顺序取第一条可用的：

1. **对象存储**（推荐）：配了 `storage_*` 就把原片 PUT 进桶里，`video_url` 用公网
   域名或预签名 GET；成片预签名 PUT 进桶。渠道所在机器不需要公网入口。
2. **内置文件服务**（兜底）：配了 `asset_public_base_url` 就在 `asset_serve_addr`
   上起一个 HTTP 服务，`GET /asset/in/<token>` 供上游拉原片，`PUT /asset/out/<token>`
   收成片。只有当次任务注册过的 token 可达，没有目录列举。要求无痕的服务器能反向
   访问到这台机器（反代、公网域名或隧道）。

两条都没配，遇到 `file://` 素材时报结构化 `CHANNEL_NOT_CONFIGURED` 并指名缺哪个键，
不会静默失败，也不会伪造结果。`CheckTask` 会在平台租账号之前先做这个预检：文件不
存在、是空文件、超过 1GiB 上限，或渠道没有出口，都直接回 `Eligible=false`。

`videos[1]` 也可以是 `file://`：成片要落到本机时，有内置文件服务就让上游直接 PUT
过来（临时文件 + rename，读到的永远是完整视频）；只有对象存储时上游先 PUT 进桶，
`Query` 再把对象取回那个路径。成片没落地时 `Query` 回可重试错误，下一次轮询再看，
不会把半个文件当成功交出去。这时回给平台的 `TaskPayload.URL` 是 `file://` 绝对路径。

| `config.json` 键 | 含义 | 缺省 |
|---|---|---|
| `storage_endpoint` | S3 兼容 endpoint，如 `https://tos-s3-cn-beijing.volces.com` | 空 |
| `storage_region` | 区域，如 `cn-beijing` | 空 |
| `storage_bucket` | 桶名 | 空 |
| `storage_access_key` / `storage_secret_key` | 桶的 AK/SK（**存储配置，不是账号凭据**） | 空 |
| `storage_prefix` | 对象键前缀，实际键是 `<prefix>/<in\|out>/<日期>/<token>.mp4` | `wuhen` |
| `storage_public_base_url` | 桶的公网/CDN 域名。留空则回预签名 GET | 空 |
| `storage_path_style` | `true` 用 `endpoint/bucket/key`，默认虚拟主机风格 | `false` |
| `storage_url_ttl_seconds` | 预签名有效期，夹在 60s–7d | 172800 |
| `storage_timeout_seconds` | 单次对象存储传输超时 | 900 |
| `asset_public_base_url` | 内置文件服务的公网根地址。留空则不开这条出口 | 空 |
| `asset_serve_addr` | 内置文件服务监听地址 | `127.0.0.1:19312` |

五个 `storage_*` 必填项（endpoint/region/bucket/AK/SK）缺任意一个，这条出口就当没配。
`asset_serve_addr` 改了要重启 worker 才会释放旧端口——`ReloadConfig` 只换公网根地址，
不会掐掉正在传输的任务。上传到桶里的原片不会自动删，按桶的生命周期规则清理。

大文件传输期间 `Submit` 会每 45 秒打一条进度心跳，避免平台把慢提交判成
`SUBMIT_STALLED`（120 秒静默就会被掐）。

> 依赖的 `channel-sdk` 必须放行 `file://` 素材：`runtime.ValidateTaskInput` 在进入插件
> 代码前就会校验 `videos[]` 的 scheme。老版本 SDK 只认 `http(s)://` 和 `data:`，插件会回
> 1001，平台随后退回原始链接。裸路径（不带 scheme）仍然是非法入参。

`prompt` 协议必填，本渠道不消费语义，填 `watermark_removal` 即可。模型用
`video_removal_std` / `video_removal_pro`。图片任务 CheckTask 直接放行；本地视频仍做可读性预检。CheckAccount 打
`/user/me` 回积分。

The method **signatures** in `channel.go` are fixed by `runtime.Channel`,
`runtime.ModelLister`, and `runtime.AccountRegistrar`. Do not change them; fill
in the bodies. Everything framework-level (response
envelopes, idempotency dedup, error normalization, heartbeat, graceful shutdown)
lives in the SDK and never appears here.

## Develop

```sh
# 1. Self-check the freshly generated scaffold. `verify` runs against the
#    built-in verify-only mock, so it passes out of the box and stays offline:
go run . verify

# 2. Implement realUpstream in upstream.go and return it from selectUpstream
#    (channel.go), built from config.json (base_url, timeout) — never env.
#    Serve is ALWAYS live: there is no mock mode and no "mode" config key.
#    Until realUpstream exists / config.json is complete, serve answers
#    upstream calls with structured CHANNEL_NOT_CONFIGURED errors.

# 3. Re-run verify until green (verify keeps using the mock; it never spends
#    provider credits):
go run . verify

# 4. Build the upload package. package.sh 自己完成编译和契约自检，入口名与版本
#    从 manifest.json 读，产物按目标架构命名，工作目录不会留下二进制：
./package.sh                          # 本机架构
GOOS=linux GOARCH=amd64 ./package.sh  # 平台部署常用
GOOS=linux GOARCH=arm64 ./package.sh
```

交叉编译时 `verify` 仍用本机架构的二进制跑——它检查的是 manifest、能力和协议，
与目标架构无关，而交叉编译的产物在本机执行不了。`CGO_ENABLED=0` 保证产物是静态
链接，落到目标机器不依赖 glibc。

### 强制约束 / MUST: no symbolic links in the upload package

Upload is **only** `.tar.gz` / `.tgz` (gzip+tar). Not zip, not a bare
`.tar`, not a folder. The archive root must contain `manifest.json`
(no extra wrapping directory).

**禁止软链接（以及硬链接、设备节点）进包。** The install gate rejects
any tar entry that is not a regular file or directory (`type 50` =
symlink). Typical footgun: `node_modules/.bin/*`.

- **MUST** use `./package.sh` (it stages with `cp -RL`, then
  `COPYFILE_DISABLE=1 tar -czf`). **禁止** `tar czf` a tree that
  still has symlinks.
- After packing, `tar -tvzf dist/*.tar.gz | awk '$1 ~ /^l/'` must be
  empty. `package.sh` runs `find "$stage" -type l` and exits non-zero
  if any symlink remains.

**Anti-pattern:** `unsupported tar entry ".../node_modules/.bin/tldts" (type 50)`.

`go run . verify` runs the **same checks** the platform runs at install time, so a
green local run means install will not fail on contract/protocol grounds.

## V1.6 contract

- **ListModels**: return the channel's `modelId` and original `modelName`.
- **Registration**: start/query the channel-owned registration flow. Replays of
  the same registration idempotency key return the same `registrationTaskId`.
- **Opaque account context**: return credential/fingerprint bytes unchanged;
  later task calls receive those bytes plus the account's fixed proxy.
- **Immediate runtime**: the platform does not run `pip install`, `npm install`
  or another build step. Include every runtime dependency/resource in the
  package or use only dependencies guaranteed on the target host.
- **Account signals**: classify account, credential, and proxy failures on
  `runtime.ChannelError.AccountSignal` so the platform can freeze or replace.

## Config file (`config.json`) and live reload

`config.json` is this channel's **freeform** settings file — the SDK imposes no
schema, so add whatever keys you need (endpoints, mail/captcha options, poll
intervals, feature flags, ...). It ships in the package root and is
read at startup by `New()` via `runtime.LoadConfig()`. Read values with
`c.config.String/Bool/Int` or decode the whole file into a typed struct with
`c.config.Unmarshal(&myCfg)`.

Operators edit this file from the platform's channel-SDK management UI. After a
save, the platform calls the **`ReloadConfig`** RPC, which re-reads the file and
**re-applies** `base_url` and the other settings (see `selectUpstream` in
`channel.go`) — no worker restart. Keep `ReloadConfig` calling
`c.config.Reload()` and then rebuilding any config-dependent client.

### 强制约束 / MUST: serve is always live; no env vars for settings

**Serve has exactly one mode: live.** There is no mock mode and no `"mode"`
config key. The built-in mock backs only the `verify` subcommand
(`NewForVerify`); `serve` can never reach it. While `config.json` is
incomplete (for example an empty `base_url`), upstream calls fail with a
structured `CHANNEL_NOT_CONFIGURED` error — the worker stays healthy, but it
never fabricates results. The packaged `config.json` ships live defaults.

**All** plugin/business settings live in `config.json`. Do **not**
`os.Getenv` / `os.LookupEnv` / `os.Environ` for base URL, mail helper,
captcha, timeouts, feature flags, or region. Do **not** document
`export FOO_BASE_URL=...` or `MIORA_SD_*` as the way to configure the
upstream.

- Allowed env is only the platform-injected contract:
  `CHANNEL_LISTEN`, `CHANNEL_CONFIG_PATH`. Do not invent more `CHANNEL_*`
  knobs. Account email/password come from `StartRegistration`, not env
  and not `config.json`.

> Account credentials do **not** belong in `config.json`. The platform's email
> pool hands `email`/`password` to `StartRegistration` per registration; keep
> only non-secret provider settings here.

## Idempotency and optional cancel

- **Idempotency**: the SDK remembers `idempotency_key → provider_task_id` and
  short-circuits duplicate submits before they reach your `Submit`. You never
  implement dedup.
- **Cancel**: optional. If you do not implement `Cancel`, the framework returns
  "unsupported" and the scheduler falls back to soft cancel. Implement the
  optional `runtime.Canceler` and set `SupportsCancel: true` only if your upstream
  can truly cancel.
