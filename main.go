// Command wuhen-removal-channel is a standalone channel SDK plugin. It compiles to a
// self-contained gRPC worker binary and depends only on the shared channel-sdk
// contract, never on the scheduler.
//
//	wuhen-removal-channel serve    run the gRPC worker (default)
//	wuhen-removal-channel verify   run local self-checks (same checks as install)
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"

	"github.com/mao-dispatch/channel-sdk/runtime"
	"github.com/mao-dispatch/channel-sdk/runtime/verify"
)

// manifestJSON is the plugin manifest, embedded so `verify` checks the real file
// and `serve` reads its version without drift.
//
//go:embed manifest.json
var manifestJSON []byte

func main() {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	switch cmd {
	case "serve":
		// Serve is always live: New() builds the real upstream from config.json
		// and reports CHANNEL_NOT_CONFIGURED until the operator completes it.
		pv := pluginVersion()
		if err := runtime.Serve(New(), runtime.ServeOptions{PluginVersion: pv}); err != nil {
			fmt.Fprintln(os.Stderr, "serve:", err)
			os.Exit(1)
		}
	case "verify":
		// Verify is the only place the built-in mock upstream is reachable.
		rep := verify.Run(verify.Input{
			Manifest:    manifestJSON,
			Channel:     NewForVerify(),
			SampleInput: SampleInput,
		})
		printReport(rep)
		if !rep.OK() {
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q (use serve|verify)\n", cmd)
		os.Exit(2)
	}
}

func pluginVersion() string {
	var m struct {
		Version string `json:"version"`
	}
	_ = json.Unmarshal(manifestJSON, &m)
	return m.Version
}

func printReport(r verify.Report) {
	if len(r.Issues) == 0 {
		fmt.Println("verify: OK — no issues")
		return
	}
	for _, i := range r.Issues {
		fmt.Printf("[%s] %s %s\n  %s\n", i.Severity, i.Code, i.Path, i.Message)
		if i.Suggestion != "" {
			fmt.Printf("  suggestion: %s\n", i.Suggestion)
		}
	}
	if r.OK() {
		fmt.Println("verify: passed with warnings")
	} else {
		fmt.Println("verify: FAILED")
	}
}
