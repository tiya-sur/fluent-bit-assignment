# Fluent Bit Custom C Plugin Assignment

## What This Is

This repository contains a **complete, working Fluent Bit log pipeline** with two custom C plugins built directly into Fluent Bit. Everything runs in a single process — no separate middleware.

**The pipeline:**
1. Reads logs from files (two different formats)
2. Parses them into structured records
3. Filters out DEBUG level
4. Adds a per-alert count field (filter plugin)
5. Batches records and collapses duplicate alerts (output plugin)
6. Sends as JSON arrays via HTTP

---

## What You Need (Ubuntu/WSL)

```bash
sudo apt-get update && sudo apt-get install -y \
  build-essential cmake git libssl-dev libsasl2-dev \
  pkg-config zlib1g-dev flex bison python3
```

---

## Quick Setup

### 1. Clone this repository

```bash
git clone https://github.com/YOUR_USERNAME/fluent-bit-assignment.git
cd fluent-bit-assignment
```

Your repo already has the plugin files. Now you need to clone Fluent Bit itself and integrate them.

### 2. Clone Fluent Bit v4.0.3

```bash
cd ~
git clone https://github.com/fluent/fluent-bit.git
cd fluent-bit
git checkout v4.0.3
```

### 3. Copy plugins into Fluent Bit

```bash
cp -r ~/fluent-bit-assignment/plugins/filter_count \
      ~/fluent-bit/plugins/

cp -r ~/fluent-bit-assignment/plugins/out_batchhttp \
      ~/fluent-bit/plugins/
```

### 4. Register plugins

Open `~/fluent-bit/plugins/CMakeLists.txt` and find this comment:
```
# Generate the header from the template
```

Add these two lines **just before** that comment:
```cmake
REGISTER_FILTER_PLUGIN("filter_count")
REGISTER_OUT_PLUGIN("out_batchhttp")
```

### 5. Build Fluent Bit with plugins

```bash
cd ~/fluent-bit
rm -rf build
mkdir build && cd build

cmake .. \
  -DFLB_FILTER_COUNT=On \
  -DFLB_OUT_BATCHHTTP=On \
  -DFLB_CONFIG_YAML=Off \
  -DFLB_DEBUG=On

make -j$(nproc)
```

**Note:** `-DFLB_CONFIG_YAML=Off` disables YAML support (not needed for this assignment).

### 6. Verify plugins built correctly

```bash
~/fluent-bit/build/bin/fluent-bit --list-plugins | grep -E "count_filter|batch_http"
```

Should show:
```
count_filter   Add composite-key alert count field to each log record
batch_http     Batching and alert-collapsing HTTP output
```

### 7. Set up working directory

```bash
mkdir -p ~/fluent-bit-plugin

# Copy config and logs
cp ~/fluent-bit-assignment/config/* ~/fluent-bit-plugin/
cp ~/fluent-bit-assignment/logs/* ~/fluent-bit-plugin/
cp ~/fluent-bit-assignment/server/server.py ~/fluent-bit-plugin/

# Update paths in config files (replace tiya2 with your username)
sed -i "s|tiya2|$(whoami)|g" ~/fluent-bit-plugin/fluent-bit-stdout.conf
sed -i "s|tiya2|$(whoami)|g" ~/fluent-bit-plugin/fluent-bit-http.conf
sed -i "s|tiya2|$(whoami)|g" ~/fluent-bit-plugin/parsers.conf
```

---

## Run the Pipeline

There are two phases. **Do Phase 1 first** to validate parsing and counting before adding HTTP.

### Phase 1 — Validate with Stdout (Required First)

This test confirms that:
- Log parsing works correctly
- Count field is added correctly
- Both log formats are handled

```bash
~/fluent-bit/build/bin/fluent-bit \
  -c ~/fluent-bit-plugin/fluent-bit-stdout.conf
```

Run for 10 seconds, then **Ctrl+C**.

**Expected output** (partial):
```json
{"level":"ERROR","file":"risk.cpp","line":"10","count":1,"message":"Position limit exceeded"}
{"level":"INFO","file":"engine.cpp","line":"20","count":1,"message":"Order received"}
{"level":"ERROR","file":"risk.cpp","line":"11","count":2,"message":"Position limit exceeded again"}
{"level":"WARNING","file":"feed.cpp","line":"30","count":1,"message":"Delayed market data"}
{"level":"ERROR","file":"risk.cpp","line":"12","count":3,"message":"Hard breach detected"}
```

**What to verify:**
- ✅ Count increments per composite key (same file+line gets count 2, 3, etc.)
- ✅ Different keys get separate counts (risk.cpp and feed.cpp have independent counts)
- ✅ No DEBUG records (grep filter removed them)
- ✅ Both log formats parsed successfully

---

### Phase 2 — Send to HTTP Server

**Terminal 1 — Start the HTTP server:**
```bash
python3 ~/fluent-bit-plugin/server.py
```

You should see:
```
[server] listening on :8080
```

**Terminal 2 — Run Fluent Bit:**
```bash
~/fluent-bit/build/bin/fluent-bit \
  -c ~/fluent-bit-plugin/fluent-bit-http.conf
```

Run for 10 seconds, then **Ctrl+C**.

**Expected server output** (Terminal 1):
```
============================================================
POST /portfolio_log_analyzer/query-handle_portfolio_alerts
============================================================
Batch of 12 record(s):
  [1]  level=ERROR file=risk.cpp line=10 count=1 msg=Position limit exceeded
  [2]  level=INFO file=engine.cpp line=20 count=1 msg=Order received
  [3]  level=ERROR file=risk.cpp line=11 count=2 msg=Position limit exceeded again;;;Sample Detail1
  [10] level=ERROR file=order_handler.py line=88 count=2 msg=Order reject count 13;;;downstream null
  [12] level=ERROR file=network.py line=41 count=1 msg=Socket timeout 7
```

**What to verify:**
- ✅ Only 1 HTTP request for 12 records (not 12 separate requests)
- ✅ `Order reject count 12` is missing — merged into `count 13` (alert collapsing works)
- ✅ `Order reject count 13` appears once with count=2 (first occurrence sent separately, second merged)
- ✅ All original fields preserved unchanged

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
