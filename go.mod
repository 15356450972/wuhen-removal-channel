module github.com/mengjun/wuhen-removal-channel

go 1.26.4

require (
	github.com/mao-dispatch/channel-sdk v0.1.0
	wuhen-sdk v0.0.0
)

require (
	github.com/santhosh-tekuri/jsonschema/v6 v6.0.2 // indirect
	golang.org/x/net v0.55.0 // indirect
	golang.org/x/sys v0.45.0 // indirect
	golang.org/x/text v0.37.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20260526163538-3dc84a4a5aaa // indirect
	google.golang.org/grpc v1.83.0 // indirect
	google.golang.org/protobuf v1.36.11 // indirect
)

// Local development: resolve the contract module from a checkout rather than a
// published version. Once channel-sdk is published, delete this line and the
// require above will fetch it normally.
replace github.com/mao-dispatch/channel-sdk => /Users/mengjun/编程/谷歌登录/mao-dispatch/sdk

// 无痕协议客户端（鉴权 / video_removal / status），与 leonardo-go 共用同一份源码。
replace wuhen-sdk => /Users/mengjun/编程/谷歌登录/leonardo-go/third_party/wuhen_sdk
