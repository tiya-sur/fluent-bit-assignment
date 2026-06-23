# Fluent Bit Custom C Plugin Assignment

## What This Is

This repository contains a **complete, working Fluent Bit log pipeline** with two custom C plugins built directly into Fluent Bit. Everything runs in a single process — no separate middleware.

**The pipeline:**
1. Reads logs from files (multiple formats, including ID-bearing *.log files)
2. Parses them into structured records
3. Filters out DEBUG level
4. Extracts strategy ID (strat_id) from log filenames via configurable regex
5. Adds a per-alert count field (filter plugin)
6. Routes %%marker%% alerts to a separate HTTP endpoint
7. Batches records and collapses duplicate alerts (output plugin)
8. Sends as JSON arrays via HTTP

---

## What You Need (Ubuntu/WSL)

```bash
sudo apt-get update && sudo apt-get install -y \
  build-essential cmake git libssl-dev libsasl2-dev \
  pkg-config zlib1g-dev flex bison python3
```

---

## Complete Setup (7 Steps)

### Step 1: Clone This Repository

```bash
git clone https://github.com/tiya-sur/fluent-bit-assignment.git
cd fluent-bit-assignment
```

### Step 2: Clone Fluent Bit v4.0.3

```bash
cd ~
git clone https://github.com/fluent/fluent-bit.git
cd fluent-bit
git checkout v4.0.3
```

### Step 3: Copy Plugins Into Fluent Bit

```bash
cp -r ~/fluent-bit-assignment/plugins/filter_count ~/fluent-bit/plugins/
cp -r ~/fluent-bit-assignment/plugins/out_batchhttp ~/fluent-bit/plugins/
```

### Step 4: Register Plugins (Critical Step)

**Open the file:**
```bash
nano ~/fluent-bit/plugins/CMakeLists.txt
```

**Find this line (should be line 487):**
```
# Generate the header from the template
```

**Add these TWO lines RIGHT BEFORE that comment line:**
```cmake
REGISTER_FILTER_PLUGIN("filter_count")
REGISTER_OUT_PLUGIN("out_batchhttp")
```

**Save and exit:** `Ctrl+X`, then `Y`, then `Enter`

**Verify it worked:**
```bash
sed -n '485,490p' ~/fluent-bit/plugins/CMakeLists.txt
```

Should show your two REGISTER lines before the comment.

### Step 5: Build Fluent Bit With Plugins

```bash
cd ~/fluent-bit
rm -rf build && mkdir build && cd build

cmake .. \
  -DFLB_FILTER_COUNT=On \
  -DFLB_OUT_BATCHHTTP=On \
  -DFLB_CONFIG_YAML=Off \
  -DFLB_DEBUG=On

make -j4
```

**Wait for build to complete (~5-10 minutes).** Should end with `[100%] Built target fluent-bit-shared`

### Step 6: Verify Plugins Registered

```bash
~/fluent-bit/build/bin/fluent-bit --version
~/fluent-bit/build/bin/fluent-bit --list-plugins | grep -E "count_filter|batch_http"
```

**Should show:**
```
count_filter   Add composite-key alert count field to each log record
batch_http     Batching and alert-collapsing HTTP output
```

If both appear → **Plugins built successfully!** ✅

### Step 7: Set Up Working Directory

```bash
mkdir -p ~/fluent-bit-plugin

# Copy all config and test files
cp ~/fluent-bit-assignment/config/* ~/fluent-bit-plugin/
cp ~/fluent-bit-assignment/logs/* ~/fluent-bit-plugin/
cp ~/fluent-bit-assignment/server/server.py ~/fluent-bit-plugin/

# Update paths in config files (replace tiya2 with your actual username)
sed -i "s|tiya2|$(whoami)|g" ~/fluent-bit-plugin/fluent-bit-stdout.conf
sed -i "s|tiya2|$(whoami)|g" ~/fluent-bit-plugin/fluent-bit-http.conf
sed -i "s|tiya2|$(whoami)|g" ~/fluent-bit-plugin/parsers.conf
```

---

## Run the Pipeline (Validation)

### Phase 1: Stdout Validation (Do This First)

```bash
~/fluent-bit/build/bin/fluent-bit \
  -c ~/fluent-bit-plugin/fluent-bit-stdout.conf
```

**Run for 10 seconds, then Ctrl+C**

**Expected output (partial):**
```json
{"level":"ERROR","file":"risk.cpp","line":"10","count":1,"message":"Position limit exceeded"}
{"level":"INFO","file":"engine.cpp","line":"20","count":1,"message":"Order received"}
{"level":"ERROR","file":"risk.cpp","line":"11","count":2,"message":"Position limit exceeded again;;;Sample Detail1"}
{"level":"ERROR","file":"risk.cpp","line":"11","count":3,"message":"Position limit exceeded again;;;Sample Detail2"}
```

**What to verify:**
- ✅ Count field present on every record
- ✅ Same file+line gets incremented count (risk.cpp:11 goes 1→2→3)
- ✅ Different keys get separate counts
- ✅ No DEBUG records (grep filter removed them)
- ✅ Both log formats parsed correctly

---

### Phase 2: HTTP Batching & Collapsing (After Phase 1 Passes)

**Terminal 1 — Start HTTP server:**
```bash
python3 ~/fluent-bit-plugin/server.py
```

You should see:
```
Server running on http://0.0.0.0:8080
```

**Terminal 2 — Run Fluent Bit:**
```bash
~/fluent-bit/build/bin/fluent-bit \
  -c ~/fluent-bit-plugin/fluent-bit-http.conf
```

Run for 10 seconds, then **Ctrl+C** in Terminal 2.

**Expected server output (Terminal 1):**
```
===== RECEIVED LOG =====
[
  {"level":"ERROR","file":"risk.cpp","line":"10","count":1,"message":"Position limit exceeded"},
  {"level":"INFO","file":"engine.cpp","line":"20","count":1,"message":"Order received"},
  {"level":"ERROR","file":"risk.cpp","line":"11","count":3,"message":"Position limit exceeded again;;;Sample Detail2"},
  ...
  {"level":"ERROR","file":"order_handler.py","line":"88","count":2,"message":"Order reject count 13;;;downstream null"},
  {"level":"ERROR","file":"network.py","line":"41","count":1,"message":"Socket timeout 7"}
]
```

**What to verify:**
- ✅ Single POST request with 12 records (not 12 separate requests)
- ✅ `Order reject count 12;;;Detail1` is **missing** — merged into count 13
- ✅ `Order reject count 13;;;Detail2` present with count=2 (first occurrence sent separately, second merged)
- ✅ All original fields preserved unchanged
- ✅ Payload is a plain JSON array

---

## How the Plugins Work

### filter_count Plugin

**Adds a `count` field to every record based on composite alert key.**

Key format: `severity|cleaned_alert_brief|file|line`

Example:
- `ERROR|order reject count|order_handler.py|88`

The `cleaned_alert_brief` is extracted from message by:
1. Take text before `;;;`
2. Strip hex addresses (`0x...`)
3. Strip digits
4. Strip `...check the file:` suffix
5. Collapse whitespace, lowercase

Two records with same key share the same counter — this matches the merge logic in the output plugin.

### batch_http Output Plugin

**Batches records and collapses duplicate alerts.**

**First Occurrence Rule:**
- When alert key seen for first time → send immediately as `[{...}]`
- Preserves original alert creation timestamp on server

**Subsequent Occurrences:**
- Buffer in memory
- Collapse: same key = latest record wins
- Flush when: count reaches 200 OR 2 seconds elapsed

**Configuration:**
```ini
[OUTPUT]
    Name              batch_http
    Match             *
    Host              127.0.0.1
    Port              8080
    path              /portfolio_log_analyzer/query-handle_portfolio_alerts
    batch_size        200
    batch_timeout_sec 2
    collapse_alerts   true
    retry_limit       3
    retry_delay_sec   1
```

---

## Understanding the Plugins

### filter_count Plugin

**What it does:**
- Adds a `count` field to every record
- The count is per **composite alert key**: `severity|cleaned_alert_brief|file|line`
  - Same file+line = same count
  - Different file or line = separate count
- This is the **same key used for collapsing** in the output plugin

**Configuration:**
```ini
[FILTER]
    Name        count_filter
    Match       *
    output_key  count
```

---

### batch_http Output Plugin

**What it does:**
1. **First occurrence rule:** When a new alert key appears for the first time, send it immediately as `[{...}]` to preserve creation timestamp
2. **Subsequent occurrences:** Merge same-key records (latest wins), buffer them, and send in batches
3. **Flush conditions:**
   - When 200 records buffered (count threshold)
   - After 2 seconds with no new records (timeout threshold)
4. **Alert collapsing:** Two records with the same composite key become one (latest version only)

**Configuration:**
```ini
[OUTPUT]
    Name              batch_http
    Match             *
    Host              127.0.0.1
    Port              8080
    path              /portfolio_log_analyzer/query-handle_portfolio_alerts
    batch_size        200
    batch_timeout_sec 2
    collapse_alerts   true
    retry_limit       3
    retry_delay_sec   1
```

---

## Repository Structure

```
fluent-bit-assignment/
├── plugins/
│   ├── filter_count/           ← Plugin 1: adds count field
│   │   ├── filter_count.c
│   │   └── CMakeLists.txt
│   └── out_batchhttp/          ← Plugin 2: batches + collapses
│       ├── out_batchhttp.c
│       └── CMakeLists.txt
├── config/
│   ├── fluent-bit-stdout.conf  ← Phase 1: validates parsing
│   ├── fluent-bit-http.conf    ← Phase 2: HTTP output
│   └── parsers.conf            ← Defines two log formats
├── logs/
│   ├── logs.txt                ← Custom trading format logs
│   └── json_logs.txt           ← JSON format logs
├── server/
│   └── server.py               ← Python HTTP receiver
├── screenshots/                ← Test results
├── README.md
└── .gitignore
```

---

## Troubleshooting

### CMake fails with YAML error

```
CMake Error: YAML development dependencies required
```

**Solution:** Add `-DFLB_CONFIG_YAML=Off` to cmake command (already shown above).

### Plugins not listed after build

```bash
~/fluent-bit/build/bin/fluent-bit --list-plugins | grep -E "count_filter|batch_http"
```

Returns nothing? Check:
1. Did you add `REGISTER_FILTER_PLUGIN` and `REGISTER_OUT_PLUGIN` lines to `~/fluent-bit/plugins/CMakeLists.txt`?
2. Did you run `make clean` or `rm -rf build` before rebuilding after editing CMakeLists.txt?

### No output in Phase 1

If you see no JSON lines:
1. Check if `logs.txt` exists: `ls -la ~/fluent-bit-plugin/logs.txt`
2. Check if config file has correct paths: `grep "home/" ~/fluent-bit-plugin/fluent-bit-stdout.conf`
3. Run with verbose logging: `~/fluent-bit/build/bin/fluent-bit -v -c ~/fluent-bit-plugin/fluent-bit-stdout.conf`

### HTTP server not receiving data

1. Is Python server running in Terminal 1? Should show `[server] listening on :8080`
2. Check if logs are being generated: `tail -f ~/fluent-bit-plugin/logs.txt`
3. Check Fluent Bit for errors in Terminal 2 output

---

## How Plugins Integrate

The two plugins are standard Fluent Bit plugins:

1. **Compile:** Written in C, compiled as part of Fluent Bit's build
2. **Register:** Plugin struct registered with CMake macros
3. **Load:** Fluent Bit loads them on startup
4. **Run:** Called for every record passing through the pipeline
5. **State:** Plugins maintain in-process memory (counters, seen keys, buffers)

No external processes. No IPC. No performance overhead. Everything runs in a single Fluent Bit process.

---

## Screenshots

See `screenshots/` directory for:
- `fluent_bit_1.png` — Fluent Bit v4.0.3 version info
- `fluent_bit_2.png` — Phase 1 stdout test (count incrementing)
- `fluent_bit_3.png` — Phase 2 HTTP batching output
- `fluent_bit_4.png` — Both plugins listed by `--list-plugins`
- `fluent_bit_5.png` — First-occurrence logic (separate sends then merged batch)
- `fluent_bit_6.png` — HTTP server receiving batches

---

## Design Details

### Why Two Plugins?

**filter_count** is a filter because it's stateless per record — it just reads fields and appends a count.

**batch_http** is an output because it needs to:
- Buffer records across multiple flushes
- Track seen alert keys (per-run state)
- Maintain HTTP connection
- Implement retry logic

Fluent Bit's filter and output plugin interfaces are designed for exactly these use cases.

### Composite Alert Key

The key format: `severity|cleaned_alert_brief|file|line`

Example:
- `ERROR|order reject count|order_handler.py|88`

The `cleaned_alert_brief` is derived from the message field by:
1. Extract text before `;;;`
2. Strip hex addresses (`0x...`)
3. Strip digits
4. Strip `...check the file:` suffix
5. Collapse whitespace, lowercase

This ensures that messages like:
- `Order reject count 12;;;detail`
- `Order reject count 13;;;detail`

...both map to the same key after digit stripping, so they collapse to one record.

### Per-Run Tracking

The `seen_keys` set in the output plugin persists for the entire Fluent Bit run. If you stop and restart Fluent Bit, the set resets. This ensures:
- First occurrence is always sent separately (creation time preserved)
- Subsequent occurrences in future batches are still merged

---

## Questions?

Check the `config/` files for documentation on each parser and filter.



