# 📄 `emptybucket_portable` – S3 Bucket Cleaner

Go-based tool for safe and efficient mass deletion of versioned objects and delete markers in an S3-compatible bucket (including NetApp ONTAP S3). Features include:

- Progress bar
- Batch parallelism with concurrency control
- Resume from previous state (`state.json`)
- TLS skip for local networks
- Logging to file and console
- Retry mechanism and error tracking

---

## 🛠 Requirements

- Go ≥ 1.20
- Access to an S3-compatible bucket (versioned or unversioned)
- Delete permissions on objects and delete markers

---

## ⚙️ Build

```bash
go build -o emptybucket emptybucket_portable.go
```

---

## 🚀 Run

```bash
./emptybucket
```

You'll be prompted to input:

- Access Key
- Secret Key
- Bucket Name
- S3 Endpoint (e.g. `http://10.0.0.10:9000`)
- Region (e.g. `us-east-1`)

---

## ⚡️ Available Flags

| Flag              | Description                                              |
|-------------------|----------------------------------------------------------|
| `--reset-state`   | Deletes `state.json` and starts a fresh execution        |
| `--timeout`       | Global timeout (in hours) for the execution              |
| `--workers`       | Number of concurrent deletion workers                    |
| `--batch-size`    | Number of objects per delete batch                       |
| `--dry-run`       | Simulates deletions without calling the S3 API           |
| `--log-level`     | Set log level: `debug`, `info`, `warn`, `error`          |
| `--estimate-eta`  | Enable approximate ETA logging based on batch rates      |

Example:
```bash
./emptybucket --reset-state --timeout 3 --workers 8 --batch-size 500
```

---

## 🧠 How It Works

1. **Initialization**
   - Prompts for credentials and endpoint (unless passed as flags)
   - Connects to the bucket with TLS verification disabled

2. **State Loading**
   - Loads `state.json` (if exists) to skip previously deleted objects

3. **Producer**
   - If versioning is enabled:
     - Streams object versions and delete markers into batches
   - If versioning is disabled:
     - Uses `ListObjectsV2` to collect all objects
   - Skips already-processed entries via `state.json`

4. **Batch Deletion**
   - Deletes in parallel using a producer–consumer model
   - Number of workers and batch size are configurable
   - Retries deletions up to 3 times on error
   - Adaptive throttling slows down on repeated failures

5. **Progress Tracking**
   - Shows live counters (deleted and error counts) updated in real-time
   - Logs flushed on every batch or signal
   - Optional approximate ETA logging when enabled

6. **State Saving**
   - Saves successfully deleted objects to `state.json`

7. **Logging**
   - Output goes to:
     - `output.log`
     - `failures.csv` (failed deletions)
     - `log_json.json` (run metadata)

---

## 🧾 Generated Files

| File             | Content                                     |
|------------------|---------------------------------------------|
| `output.log`     | Execution log                               |
| `log_json.json`  | Bucket metadata (versioning, timestamp, etc.)|
| `failures.csv`   | Unsuccessful deletions                      |
| `state.json`     | List of previously processed objects        |
| `metrics.json`   | Execution summary (duration, success/failure stats)     |

---

## 🧪 Tested With

- ✅ NetApp ONTAP S3 9.15
- ✅ Versioned and unversioned buckets
- ✅ Local network S3 access (self-signed TLS)

---

## 📌 Future Improvements

- [x] `--dry-run` support
- [ ] Prefix filtering (`--prefix`)
- [x] Fully non-interactive mode (flags for credentials)
- [ ] Prometheus or structured metrics export
- [ ] Adaptive worker scaling
- [x] JSON metrics export (`metrics.json`)

---

## 👤 Author

Maintained by **@nikosubra**  
Environment: GENDATA – System Integrator / Sysadmin