# Fluent Bit Custom Plugin Assignment

## Overview

This project implements a complete Fluent Bit pipeline with two custom plugins written in C, built and linked directly into the Fluent Bit process. There is no separate middleware process — all batching, collapsing, and filtering logic runs inside Fluent Bit as native C plugins.

**One-line summary:** Source-style log records are read from file, parsed into structured fields, enriched with a per-level count, then sent as fewer larger JSON-array HTTP requests — with same-key alerts collapsed to their latest version before sending.

---

## Architecture

```
logs.txt      ──► [INPUT] tail ──► custom_parser
                                        │
json_logs.txt ──► [INPUT] tail ──► json_parser
                                        │
                                        ▼
                              [FILTER] grep          ← config-only: drops DEBUG
                                        │
                                        ▼
                              [FILTER] count_filter  ← C plugin: adds count field
                                        │
                                        ▼
                              [OUTPUT] batch_http    ← C plugin: batches + collapses
                                        │
                                        ▼
                               server.py             ← Python HTTP receiver
```

### Fluent Bit Pipeline Concepts

Fluent Bit processes logs through four stages:

- **Input** — reads raw data from a source (file, socket, etc.) and emits records tagged with a routing key
- **Parser** — converts raw text lines into structured key-value maps; runs inside the input plugin
- **Filter** — transforms or enriches records in-flight; can add fields, drop records, or route based on content
- **Output** — delivers the final records to a destination (stdout, HTTP, file, etc.)

Tags control routing. Each input assigns a tag (e.g. `mylogs`, `jsonlogs`) and filters/outputs use `Match` patterns to select which tagged records to process.

**No middleware.py. No separate process. Everything runs inside Fluent Bit.**

---

## Repository Structure

```
fluent-bit-assignment/
├── plugins/
│   ├── filter_count/
│   │   ├── filter_count.c       # Doc 1: per-level log counter filter plugin
│   │   └── CMakeLists.txt
│   └── out_batchhttp/
│       ├── out_batchhttp.c      # Doc 2: batching + alert collapsing output plugin
│       └── CMakeLists.txt
├── config/
│   ├── parsers.conf             # Two parsers: custom regex + JSON
│   ├── fluent-bit-stdout.conf   # Phase 5: stdout validation config
│   └── fluent-bit-http.conf     # Phase 7: HTTP output config
├── logs/
│   ├── logs.txt                 # Sample logs in custom trading format
│   └── json_logs.txt            # Sample logs in JSON format
├── server/
│   └── server.py                # Simple Python HTTP receiver
└── README.md
```

---

## Prerequisites

Ubuntu / WSL:

```bash
sudo apt-get update && sudo apt-get install -y \
  build-essential cmake git libssl-dev libsasl2-dev \
  pkg-config zlib1g-dev flex bison python3
```

---

## Setup and Build

### 1. Clone Fluent Bit source

```bash
cd ~
git clone https://github.com/fluent/fluent-bit.git
cd fluent-bit
git checkout v3.2.4
```

### 2. Clone this repository and copy plugins

```bash
git clone https://github.com/YOUR_USERNAME/fluent-bit-assignment.git
cd fluent-bit-assignment

cp -r plugins/filter_count  ~/fluent-bit/plugins/filter_count
cp -r plugins/out_batchhttp ~/fluent-bit/plugins/out_batchhttp
```

### 3. Register plugins with Fluent Bit's build system

Open `~/fluent-bit/plugins/CMakeLists.txt` and add these two lines just before the comment `# Generate the header from the template`:

```cmake
REGISTER_FILTER_PLUGIN("filter_count")
REGISTER_OUT_PLUGIN("out_batchhttp")
```

### 4. Build

```bash
cd ~/fluent-bit
mkdir -p build && cd build

cmake .. \
  -DFLB_FILTER_COUNT=On \
  -DFLB_OUT_BATCHHTTP=On \
  -DFLB_BATCHHTTP=On \
  -DFLB_OUT_BATCH_HTTP=On \
  -DFLB_DEBUG=On

make -j$(nproc)
```

### 5. Verify plugins are registered

```bash
~/fluent-bit/build/bin/fluent-bit --list-plugins | grep -E "count_filter|batch_http"
```

Expected:
```
  count_filter    Add per-level running count field to each log record
  batch_http      Batching and alert-collapsing HTTP output
```

### 6. Set up working directory

```bash
mkdir -p ~/fluent-bit-plugin

sed -i "s|tiya2|$(whoami)|g" config/fluent-bit-stdout.conf
sed -i "s|tiya2|$(whoami)|g" config/fluent-bit-http.conf
sed -i "s|tiya2|$(whoami)|g" config/parsers.conf

cp config/* ~/fluent-bit-plugin/
cp logs/*   ~/fluent-bit-plugin/
cp server/server.py ~/fluent-bit-plugin/
```

---

## Running the Pipeline

### Phase 5 — Validate with stdout first (mandatory)

This step must be done before HTTP. It confirms parsing and counting are correct before adding any network complexity.

```bash
~/fluent-bit/build/bin/fluent-bit \
  -c ~/fluent-bit-plugin/fluent-bit-stdout.conf
```

Expected output:

```json
{"level":"ERROR","file":"risk.cpp","line":"10","message":"Position limit exceeded","count":1}
{"level":"INFO","file":"engine.cpp","line":"20","message":"Order received","count":1}
{"level":"ERROR","file":"risk.cpp","line":"11","message":"Position limit exceeded again","count":2}
{"level":"WARNING","file":"feed.cpp","line":"30","message":"Delayed market data","count":1}
{"level":"ERROR","file":"risk.cpp","line":"12","message":"Hard breach detected","count":3}
{"level":"INFO","file":"engine.cpp","line":"21","message":"Order filled","count":2}
{"level":"CRITICAL","file":"monitor.cpp","line":"5","message":"System overload","count":1}
```

What this proves:
- Parsing extracts all five fields correctly from both log formats
- `count` increments independently per level — ERROR reaches 3, INFO reaches 2
- DEBUG records are absent — config-level grep filter dropped them
- Both log formats (custom regex + JSON) produce correctly enriched records

Press `Ctrl+C` when done.

### Phase 7 — Send to HTTP server

**Terminal 1:**
```bash
python3 ~/fluent-bit-plugin/server.py
```

**Terminal 2:**
```bash
~/fluent-bit/build/bin/fluent-bit \
  -c ~/fluent-bit-plugin/fluent-bit-http.conf
```

Expected server output:

```
============================================================
POST /portfolio_log_analyzer/query-handle_portfolio_alerts
============================================================
Batch of 12 record(s):
  [1]  level=ERROR    file=risk.cpp          line=10  count=1  msg=Position limit exceeded
  [2]  level=INFO     file=engine.cpp        line=20  count=1  msg=Order received
  [3]  level=ERROR    file=risk.cpp          line=11  count=2  msg=Position limit exceeded again
  [4]  level=WARNING  file=feed.cpp          line=30  count=1  msg=Delayed market data
  [5]  level=ERROR    file=risk.cpp          line=12  count=3  msg=Hard breach detected
  [6]  level=INFO     file=engine.cpp        line=21  count=2  msg=Order filled
  [7]  level=CRITICAL file=monitor.cpp       line=5   count=1  msg=System overload
  [8]  level=ERROR    file=risk.cpp          line=20  count=4  msg=New breach detected
  [9]  level=INFO     file=engine.cpp        line=30  count=3  msg=Recovery started
  [10] level=ERROR    file=order_handler.py  line=88  count=6  msg=Order reject count 13
  [11] level=INFO     file=engine.py         line=20  count=4  msg=Strategy ready
  [12] level=ERROR    file=network.py        line=41  count=7  msg=Socket timeout 7
```

What this proves:
- 12 records arrived in **1 HTTP request** instead of 12 separate requests — request count reduced
- `Order reject count 12` is absent — collapsed into `count 13` using composite alert key — payload object count reduced
- All original field names and values are preserved unchanged
- Payload is a plain JSON array — no wrapper object added

---

## Design Note

### How the parsers work

`custom_parser` uses a regex to extract five fields from trading-style log lines:

```
2026-03-21 10:15:01,123 : ERROR : [risk.cpp : 10] : Position limit exceeded
```

Fields extracted: `time`, `level`, `file`, `line`, `message`.

`json_parser` uses Fluent Bit's built-in JSON decoder for structured log files where each line is a self-contained JSON object with the same fields. Two parsers demonstrate that the same downstream plugin works regardless of log source format — a realistic requirement when multiple services log differently.

### What the filter plugin receives

Each record arrives in `cb_filter` as a MessagePack buffer. Records are encoded as two-element arrays: `[timestamp, {field: value, ...}]`. The plugin iterates through the buffer record by record, decodes each map, guards every key access with a type check (`MSGPACK_OBJECT_STR`) before calling `strncmp` to avoid segfaults on non-string keys, then re-encodes the map with the `count` field appended.

### How the filter plugin maintains counters

Counters live in a `struct count_ctx` allocated per plugin instance in `cb_init` and freed in `cb_exit`. Levels supported: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`. Any missing or unrecognised level maps to `UNKNOWN` and still gets counted. Using a context struct (not global variables) means multiple filter instances never share or corrupt each other's state.

### Bonus feature — configurable output key (Phase 8)

The field name added to each record defaults to `count` but is configurable via the `output_key` property:

```ini
[FILTER]
    Name        count_filter
    Match       *
    output_key  level_count
```

### Why stdout was tested first

Stdout validation confirms that parsing extracts the right fields and counting increments correctly before adding any HTTP layer. A bug in parsing shows up immediately as a missing field in the terminal. This is the correct debugging discipline — validate each stage in isolation before connecting the next one.

### How the final payload reaches the HTTP receiver

Fluent Bit calls `cb_flush` on the output plugin whenever it has buffered records ready to deliver. The plugin adds each incoming record to an in-memory array. When a flush condition is met (count or timeout), the buffer is serialized as a JSON array and sent in a single HTTP POST to `server.py`. The server prints each batch and responds HTTP 200.

### Where config alone was enough vs where a plugin was needed

The `grep` filter in config was enough to drop DEBUG records (Phase 3) — a simple field match needs no custom code.

A plugin was needed for stateful per-level counting (Phase 4) because Fluent Bit has no built-in mechanism for maintaining running counters across records. A plugin was also needed for batching and alert collapsing (Doc 2) because these require in-process memory state, timeout tracking, composite key normalisation logic, and HTTP client calls — none of which can be expressed in Fluent Bit config.

---

## Flush Logic

The output plugin flushes the buffer when either condition is met:

1. **Count threshold** — buffered record count reaches `batch_size` (default 200). If 200 records arrive quickly, the plugin flushes immediately without waiting for the timer.

2. **Timeout threshold** — the oldest buffered record has been waiting longer than `batch_timeout_sec` (default 2 seconds). If only a few records arrive and no more come, the plugin flushes after the timeout so records are never stuck indefinitely.

Both conditions are checked inside `cb_flush` after every record is added to the buffer. On shutdown, `cb_exit` performs a final flush of any remaining buffered records before freeing memory.

If snapshot collapsing is enabled, the plugin may receive 200 raw records but send fewer than 200 JSON objects because same-key records were merged before the flush.

---

## Failure Handling

If the HTTP send fails:

- The failure is logged with attempt number, target URL, and error reason
- The plugin retries up to `retry_limit` times with `retry_delay_sec` seconds between attempts
- After exhausting all retries, the batch is logged as dropped and the buffer is cleared so the plugin can continue processing new records
- Fluent Bit never crashes or hangs on network failure

On shutdown with buffered records, `cb_exit` attempts one final flush. If the server is still unavailable, the drop is logged explicitly so nothing is silently lost.

---

## Plugin Configuration Reference

### filter_count

| Property | Default | Description |
|---|---|---|
| `output_key` | `count` | Name of the field added to each record |

### batch_http (out_batchhttp)

| Property | Default | Description |
|---|---|---|
| `host` | `127.0.0.1` | HTTP server hostname |
| `port` | `8080` | HTTP server port |
| `path` | `/` | HTTP request URI path |
| `batch_size` | `200` | Flush when buffer reaches this many records |
| `batch_timeout_sec` | `2` | Flush when oldest buffered record exceeds this age in seconds |
| `collapse_alerts` | `false` | Enable alert collapsing by composite key |
| `retry_limit` | `3` | Number of retries on HTTP send failure |
| `retry_delay_sec` | `1` | Seconds to wait between retry attempts |

---

## Test Cases

### Test 1 — Timeout flush
Set `batch_size 200`, `batch_timeout_sec 2`. Send only 2 records. After ~2 seconds exactly 1 HTTP request arrives containing those 2 records. Plugin did not wait for 200 records.

### Test 2 — Count flush
Set `batch_size 5`, `batch_timeout_sec 10`. Send 5 records quickly. Plugin flushes immediately on reaching the count threshold without waiting for the 10 second timeout.

### Test 3 — Alert collapsing by composite key
With `collapse_alerts true`, send:
```json
{"message":"Order reject count 12","level":"ERROR","file":"order_handler.py","line":88}
{"message":"Order reject count 13","level":"ERROR","file":"order_handler.py","line":88}
{"message":"Socket timeout 7","level":"ERROR","file":"network.py","line":41}
```
Server receives 2 records not 3. `Order reject count 12` is gone — both order records share the same composite key (`ERROR|order reject count|order_handler.py|88`) after digit stripping, so only the latest survives. `Socket timeout 7` has a different key and is kept separately.

### Test 4 — Field preservation
All original fields (`message`, `level`, `file`, `line`, `timestamp`, `source_file`) are present in the output with original values unchanged. No wrapper object added. No field renamed.

### Test 5 — Failure handling
With the server stopped, the plugin logs each failed attempt and retries `retry_limit` times. After exhausting retries it logs the drop clearly. Fluent Bit does not crash or hang.

---

## Screenshots

###  stdout test showing count incrementing
![Fluent Bit](screenshots/fluent_bit_1.jpeg)

---

### HTTP test showing batch + collapsing 
![Stdout Output](screenshots/fluent_bit_2.jpeg)

---

### Both plugins listed:
![HTTP Output](screenshots/fluent_bit_3.jpeg)
