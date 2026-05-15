# 📄 `emptybucket_portable` — S3 Bucket Cleaner

Go-based tool for safe and efficient mass deletion of objects, versions, and delete markers in any S3-compatible bucket (including NetApp ONTAP S3, MinIO, Ceph). Ships with three interchangeable user interfaces (plain CLI, TUI, local Web UI) and two deletion engines (Go SDK or shelled-out `aws` CLI).

---

## ✨ Features

- **Three UIs** sharing one core orchestrator:
  - `cli` — classic stdin prompts + live single-line progress
  - `tui` — full-screen terminal UI (Bubble Tea) with form, live progress bar, scrolling deletion log
  - `web` — local browser UI on `127.0.0.1:8765` with Server-Sent Events for real-time updates
- **Two deletion engines**:
  - `sdk` — Go AWS SDK v2 with concurrent batched `DeleteObjects`
  - `awscli` — shells out to `aws s3 rm --recursive` (unversioned) or parallel `aws s3api delete-objects` (versioned)
  - `auto` — picks `awscli` when the binary is on `PATH`, otherwise `sdk`
- **Pre-deletion inventory**: object count, top-level folder count, total size, plus versions and delete markers on versioned buckets
- **Real ETA**: `(total - deleted) / throughput`, refreshed every 500 ms
- **Live deletion stream**: every key being deleted is shown in all three UIs
- **Versioned bucket support** via `ListObjectVersions` (both versions and delete markers are queued)
- **Configurable retry + adaptive throttling** on transient errors
- **Optional TLS-skip** for self-signed local endpoints (`--insecure`)
- **STS session-token** support (`--session-token`) for temporary credentials
- **Input validation + endpoint normalization** (auto-prepends `https://`, strips `s3://` prefixes)
- **In-memory credentials only** — nothing is written to disk
- Outputs (when `--output-dir` is set): `output.log`, `failures.csv`, `metrics.json`

---

## 🛠 Requirements

- Go ≥ 1.24 (for building from source)
- An S3-compatible endpoint with delete permissions on the target bucket
- Optional: `aws` CLI v2 on `PATH` if you want to use the `awscli` engine

---

## ⚙️ Build

```bash
go build -o emptybucket-portable .
```

Use the package path `.` (not the file path) so Go embeds VCS info — `--version` then reports the commit SHA, dirty flag, and build time.

---

## 🚀 Run

The tool offers three user interfaces, selected with `--ui`.

### 1. Web UI (recommended for interactive use)

```bash
./emptybucket-portable --ui=web
# Web UI listening on http://127.0.0.1:8765
```

Open the URL in any browser. Fill in:

- Endpoint (e.g. `https://s3.example.com`)
- Region (default `us-east-1`)
- Bucket name
- Access Key / Secret Key
- Engine (`sdk` / `awscli` / `auto`)
- Workers and batch size
- Optional: dry-run

Click **Start**. The page streams the inventory scan, live deletions, throughput, ETA, and final summary.

The server binds to loopback only. Change the address with `--web-addr`:

```bash
./emptybucket-portable --ui=web --web-addr=127.0.0.1:9000
```

### 2. TUI (terminal full-screen)

```bash
./emptybucket-portable --ui=tui
```

- `Tab` / `Shift+Tab` — move between fields
- `Enter` on the engine row — cycle `sdk` → `awscli` → `auto`
- `Space` on the dry-run row — toggle
- `Ctrl+S` — start
- `Ctrl+C` / `Esc` — quit

### 3. Plain CLI (default)

```bash
./emptybucket-portable
```

You'll be prompted for: Access Key, Secret Key, Bucket name, Endpoint, Region. Progress is rendered as a single self-updating line:

```
✅ 12345/200000 (6.2%) | ETA 1m23s | path/to/last-key
```

Combine with flags for non-interactive defaults:

```bash
./emptybucket-portable --engine=auto --workers=8 --batch-size=500
```

---

## ⚡️ Flags

| Flag               | Default          | Description                                                            |
|--------------------|------------------|------------------------------------------------------------------------|
| `--ui`             | `cli`            | User interface: `cli` \| `tui` \| `web`                                |
| `--web-addr`       | `127.0.0.1:8765` | Bind address for `--ui=web`                                            |
| `--engine`         | `sdk`            | Deletion engine: `sdk` \| `awscli` \| `auto`                           |
| `--workers`        | `4`              | Concurrent deletion workers                                            |
| `--batch-size`     | `200`            | Objects per delete batch (clamped to S3 max of 1000)                   |
| `--retries`        | `3`              | Retry attempts per delete batch on transient errors                    |
| `--prefix`         | _empty_          | Restrict deletion to keys under this prefix (e.g. `logs/`)             |
| `--scan-concurrency` | `8`            | Parallel workers for the inventory scan                                |
| `--scan-strategy`  | `auto`           | `auto` \| `serial` \| `delimiter` \| `sharded`                         |
| `--skip-inventory` | `false`          | Skip the inventory scan; deletion starts immediately (no ETA, no %)    |
| `--timeout`        | `36`             | Global execution timeout in hours                                      |
| `--dry-run`        | `false`          | Simulate deletions; no objects removed                                 |
| `--insecure`       | `false`          | Skip TLS certificate verification (self-signed endpoints only)         |
| `--session-token`  | _empty_          | Optional STS session token (for temporary credentials)                 |
| `--output-dir`     | `.`              | Where `failures.csv` and `metrics.json` are written; empty disables    |
| `--log-level`      | `info`           | `debug` \| `info` \| `warn` \| `error`                                 |
| `--version`        | _flag_           | Print build version (commit SHA, dirty flag, build time) and exit     |

Example — fast cleanup with auto engine selection:

```bash
./emptybucket-portable --ui=cli --engine=auto --workers=8 --batch-size=1000
```

---

## 🧠 How It Works

1. **Inventory scan** (`lister.ParallelScan`) — a concurrent pass over the bucket. The default `--scan-strategy=auto` first runs a cheap `ListObjectsV2(Delimiter="/")` discovery: if it finds ≥4 top-level prefixes the scan parallelizes per folder; otherwise it falls back to 256 single-byte prefix shards. Either way, `--scan-concurrency` worker goroutines list pages in parallel. Versioned buckets list versions/markers in the same parallel fashion. Use `--scan-strategy=serial` to restore the legacy single-threaded scan.
2. **Engine selection** — `auto` resolves to `awscli` when the binary is on `PATH`, otherwise `sdk`.
3. **Listing** — `produceFlat` (`ListObjectsV2`) for unversioned buckets, `produceVersioned` (`ListObjectVersions`, including delete markers) for versioned buckets. Batches are streamed on a channel.
4. **Deletion** —
   - **SDK engine**: worker pool with semaphore-bounded concurrency, `DeleteObjects` per batch, 3× retry, adaptive throttle when consecutive errors exceed a threshold.
   - **AWS CLI engine, unversioned**: single `aws s3 rm s3://bucket --recursive` invocation; stdout is parsed for `delete: s3://bucket/key` lines.
   - **AWS CLI engine, versioned**: versions listed via the SDK, batches written to temp JSON, deleted in parallel via `aws s3api delete-objects --delete file://payload.json`.
5. **Event stream** — every successful or failed deletion produces a `DeletionEvent` consumed by the UI for real-time display. Stats events are emitted every 500 ms with throughput and ETA.
6. **Reporting** — failed deletions are written to `failures.csv`; a final `metrics.json` snapshot includes inventory totals, deleted/error counts, and run duration.

---

## 🧾 Generated Files

Written under `--output-dir` (default: current directory). Pass `--output-dir=""` to disable artifact writing entirely.

| File             | Content                                                       |
|------------------|---------------------------------------------------------------|
| `output.log`     | Execution log                                                 |
| `failures.csv`   | `Key,VersionId,Reason` for every deletion that failed         |
| `metrics.json`   | Final summary: duration, counts, inventory totals, engine, bucket |

---

## 🔐 Security Notes

- TLS verification is on by default. Pass `--insecure` (or check the corresponding box in the TUI/Web UI) to skip verification — needed for self-signed local endpoints (e.g. on-prem ONTAP).
- Credentials entered via the CLI prompts, Web UI, or TUI exist only in process memory. Nothing is persisted to disk.
- The Web UI binds to `127.0.0.1` by default; expose it on other interfaces only on trusted networks.

---

## 🧪 Tested With

- NetApp ONTAP S3 9.15 (versioned and unversioned)
- MinIO (local)
- Self-signed TLS endpoints on private networks

---

## 📌 Roadmap

- [x] `--dry-run`
- [x] Fully non-interactive CLI flags
- [x] JSON metrics export
- [x] Real ETA with throughput
- [x] Pre-deletion inventory
- [x] Web UI and TUI front-ends
- [x] AWS CLI engine
- [x] Configurable retries, session-token, opt-in TLS skip
- [x] Unit tests for request validation, artifact writing, throughput, inventory
- [x] `--prefix` filtering
- [x] Parallel inventory scan with auto strategy (delimiter / byte-shard / serial)
- [ ] Resume from previous run state
- [ ] Prometheus / OTEL metrics export
- [ ] Adaptive worker scaling

---

## 👤 Author

Maintained by **@nikosubra**
Environment: GENDATA — System Integrator / Sysadmin
