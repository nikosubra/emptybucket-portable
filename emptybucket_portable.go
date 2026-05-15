// emptybucket_portable is the CLI entrypoint. It dispatches to one of three
// user interfaces (cli, tui, web) and, for the cli case, drives the shared
// runner orchestrator and prints a live progress line.
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"runtime/debug"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nikosubra/emptybucket-portable/logger"
	"github.com/nikosubra/emptybucket-portable/runner"
	"github.com/nikosubra/emptybucket-portable/tui"
	"github.com/nikosubra/emptybucket-portable/webui"
)

// versionString returns a single-line build identifier. When the binary is
// built from a clean git checkout `go build` embeds VCS info via
// runtime/debug.ReadBuildInfo; otherwise we fall back to a generic marker.
func versionString() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "emptybucket-portable (unknown build)"
	}
	var rev, dirty, when, goVer string
	goVer = info.GoVersion
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
			if len(rev) > 12 {
				rev = rev[:12]
			}
		case "vcs.modified":
			if s.Value == "true" {
				dirty = "-dirty"
			}
		case "vcs.time":
			when = s.Value
		}
	}
	if rev == "" {
		rev = "devel"
	}
	return fmt.Sprintf("emptybucket-portable %s%s (built %s, %s)", rev, dirty, when, goVer)
}

func main() {
	timeoutHours := flag.Int("timeout", 36, "Global timeout for execution in hours")
	workers := flag.Int("workers", 4, "Number of concurrent deletion workers")
	batch := flag.Int("batch-size", 200, "Number of objects per delete batch (max 1000)")
	dryRun := flag.Bool("dry-run", false, "Simulate deletions without actually deleting objects")
	logLevel := flag.String("log-level", "info", "Set log level: debug, info, warn, error")
	engine := flag.String("engine", "sdk", "Deletion engine: sdk | awscli | auto")
	uiMode := flag.String("ui", "cli", "User interface: cli | tui | web")
	webAddr := flag.String("web-addr", "127.0.0.1:8765", "Bind address for --ui=web")
	insecure := flag.Bool("insecure", false, "Skip TLS certificate verification (use only for self-signed local endpoints)")
	outDir := flag.String("output-dir", ".", "Directory where failures.csv and metrics.json are written; empty disables artifact writing")
	sessionToken := flag.String("session-token", "", "Optional STS session token (for temporary credentials)")
	retries := flag.Int("retries", 3, "Retry attempts per delete batch on transient errors")
	prefix := flag.String("prefix", "", "Optional key prefix filter; only matching keys are deleted (e.g. 'logs/')")
	scanConcurrency := flag.Int("scan-concurrency", 8, "Parallel workers for the inventory scan")
	scanStrategy := flag.String("scan-strategy", "auto", "Scan strategy: auto | serial | delimiter | sharded")
	skipInventory := flag.Bool("skip-inventory", false, "Skip the inventory scan; start deletion immediately (no ETA, no progress %)")
	showVersion := flag.Bool("version", false, "Print version information and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(versionString())
		return
	}

	switch *uiMode {
	case "tui":
		if err := tui.Run(); err != nil {
			fmt.Fprintf(os.Stderr, "TUI error: %v\n", err)
			os.Exit(1)
		}
		return
	case "web":
		ctxWeb, cancelWeb := context.WithCancel(context.Background())
		defer cancelWeb()
		sigsWeb := make(chan os.Signal, 1)
		signal.Notify(sigsWeb, os.Interrupt, syscall.SIGTERM)
		go func() { <-sigsWeb; cancelWeb() }()
		fmt.Printf("Web UI listening on http://%s\n", *webAddr)
		srv := webui.New()
		if err := srv.Serve(ctxWeb, *webAddr); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "web UI error: %v\n", err)
			os.Exit(1)
		}
		return
	}

	// CLI mode.
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*timeoutHours)*time.Hour)
	defer cancel()
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	go func() {
		s := <-sigs
		fmt.Fprintf(os.Stderr, "\n%s received, cancelling...\n", s)
		cancel()
	}()

	logger.SetLevel(*logLevel)

	// Open log files in the output dir (or skip if empty).
	if *outDir != "" {
		if err := os.MkdirAll(*outDir, 0o755); err != nil {
			fmt.Fprintf(os.Stderr, "cannot create output dir: %v\n", err)
			os.Exit(1)
		}
		logFile, err := os.Create(*outDir + "/output.log")
		if err == nil {
			defer logFile.Close()
			logger.Init(logFile)
		}
	}

	fmt.Println("emptybucket — bucket cleanup")
	req := promptForRequest(*engine, *workers, *batch, *dryRun, *insecure)
	req.SessionToken = *sessionToken
	req.Retries = *retries
	req.Prefix = *prefix
	req.ScanConcurrency = *scanConcurrency
	req.ScanStrategy = *scanStrategy
	req.SkipInventory = *skipInventory

	events := make(chan runner.Event, 256)
	resultCh := make(chan runner.Result, 1)
	go func() {
		resultCh <- runner.Run(ctx, req, events)
	}()

	displayCLIProgress(events)

	res := <-resultCh
	printCLISummary(res)
	if *outDir != "" {
		if err := runner.WriteArtifacts(*outDir, req, res); err != nil {
			fmt.Fprintf(os.Stderr, "failed to write artifacts: %v\n", err)
		} else if len(res.FailedKeys) > 0 {
			fmt.Printf("📄 Failures written to %s/failures.csv\n", *outDir)
		}
	}
	if res.Errors > 0 {
		os.Exit(2)
	}
}

// promptForRequest reads connection settings from stdin and returns a runner
// Request seeded with CLI-flag defaults.
func promptForRequest(engine string, workers, batch int, dryRun, insecure bool) runner.Request {
	read := func(label string) string {
		fmt.Print(label + ": ")
		var v string
		fmt.Scanln(&v)
		return strings.TrimSpace(v)
	}
	return runner.Request{
		AccessKey: read("Enter AWS Access Key"),
		SecretKey: read("Enter AWS Secret Key"),
		Bucket:    read("Enter Bucket name"),
		Endpoint:  read("Enter S3 Endpoint"),
		Region:    read("Enter Region (default us-east-1)"),
		Engine:    engine,
		Workers:   workers,
		BatchSize: batch,
		DryRun:    dryRun,
		Insecure:  insecure,
	}
}

// displayCLIProgress consumes events from the runner and renders a single
// self-updating progress line. Returns when the events channel closes.
func displayCLIProgress(events <-chan runner.Event) {
	var latest atomic.Value
	latest.Store("")
	var lastStats runner.Stats
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	render := func() {
		k, _ := latest.Load().(string)
		if len(k) > 60 {
			k = "..." + k[len(k)-57:]
		}
		if lastStats.Total > 0 {
			pct := float64(lastStats.Deleted) / float64(lastStats.Total) * 100
			fmt.Printf("\r✅ %d/%d (%.1f%%) | %.1f/s | ETA %s | %s\033[K",
				lastStats.Deleted, lastStats.Total, pct,
				lastStats.ObjectsPerSec, durHuman(lastStats.ETA), k)
		} else {
			fmt.Printf("\r✅ %d | %.1f/s | %s\033[K", lastStats.Deleted, lastStats.ObjectsPerSec, k)
		}
	}

	for {
		select {
		case <-ticker.C:
			render()
		case ev, ok := <-events:
			if !ok {
				render()
				fmt.Println()
				return
			}
			switch ev.Kind {
			case runner.EventStarted:
				fmt.Println("▶ " + ev.Message)
			case runner.EventScanProgress:
				if ev.Scan != nil {
					sp := ev.Scan
					if sp.ShardsTotal > 0 {
						fmt.Printf("\r🔍 Scanning %d/%d shards | %s keys observed\033[K",
							sp.ShardsDone, sp.ShardsTotal, formatInt(sp.KeysScanned))
					}
				}
			case runner.EventInventory:
				if ev.Inventory != nil {
					iv := ev.Inventory
					fmt.Printf("\r\033[K📦 Objects: %d | 📁 Top-level folders: %d | 💾 Size: %s (scan %s)\n",
						iv.TotalObjects, iv.TopLevelFolders, runner.HumanBytes(iv.TotalSizeBytes), iv.Elapsed.Truncate(time.Millisecond))
					if iv.VersionedObjects > 0 {
						fmt.Printf("🗂  Versions: %d | 🪦 Delete markers: %d\n", iv.VersionedObjects, iv.DeleteMarkers)
					}
				}
			case runner.EventDeletion:
				if ev.Deletion != nil {
					latest.Store(ev.Deletion.Key)
				}
			case runner.EventStats:
				if ev.Stats != nil {
					lastStats = *ev.Stats
				}
			case runner.EventError:
				fmt.Println()
				fmt.Fprintln(os.Stderr, "✗ "+ev.Message)
			case runner.EventFinished:
				if ev.Stats != nil {
					lastStats = *ev.Stats
				}
			}
		}
	}
}

func printCLISummary(res runner.Result) {
	fmt.Printf("⏱  Duration: %s\n", res.Duration.Truncate(time.Second))
	fmt.Printf("✅ Deleted: %d\n", res.Deleted)
	if res.Errors > 0 {
		fmt.Printf("❌ Errors: %d\n", res.Errors)
	}
	if res.Inventory != nil && res.Deleted == 0 && res.Errors == 0 {
		if (res.Versioned && res.Inventory.VersionedObjects+res.Inventory.DeleteMarkers == 0) ||
			(!res.Versioned && res.Inventory.TotalObjects == 0) {
			fmt.Println("ℹ️  Bucket was already empty.")
		}
	}
}

func durHuman(d time.Duration) string {
	if d <= 0 {
		return "—"
	}
	return d.Truncate(time.Second).String()
}

func formatInt(n int64) string {
	if n < 1000 {
		return fmt.Sprintf("%d", n)
	}
	// Lightweight thousands separator without importing message/format packages.
	s := fmt.Sprintf("%d", n)
	var out []byte
	for i, c := range []byte(s) {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, c)
	}
	return string(out)
}
