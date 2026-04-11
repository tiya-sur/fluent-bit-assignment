#include <fluent-bit/flb_output.h>
#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_http_client.h>
#include <fluent-bit/flb_upstream.h>
#include <msgpack.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#define MAX_BATCH_SIZE  4096
#define MAX_KEY_LEN     512
#define MAX_FIELD_LEN   1024
#define MAX_HOST_LEN    256
#define MAX_PATH_LEN    512
#define MAX_JSON_RECORD 4096

struct buf_record {
    char alert_key[MAX_KEY_LEN];
    char json[MAX_JSON_RECORD];
    int  used;
};

struct batch_ctx {
    char   host[MAX_HOST_LEN];
    int    port;
    char   path[MAX_PATH_LEN];
    int    batch_size;
    int    batch_timeout_sec;
    int    collapse_alerts;
    int    retry_limit;
    double retry_delay_sec;

    struct buf_record records[MAX_BATCH_SIZE];
    int    record_count;
    time_t oldest_ts;

    pthread_mutex_t     lock;
    struct flb_upstream *upstream;
};

static void safe_strcpy(char *dst, const char *src, size_t n)
{
    if (!src) { dst[0] = '\0'; return; }
    strncpy(dst, src, n - 1);
    dst[n - 1] = '\0';
}

static void derive_brief(const char *msg, char *out, size_t n)
{
    const char *sep = strstr(msg, ";;;");
    if (sep) {
        size_t l = (size_t)(sep - msg);
        if (l >= n) l = n - 1;
        memcpy(out, msg, l); out[l] = '\0';
    } else {
        safe_strcpy(out, msg, n);
    }
}

static void clean_brief(char *s)
{
    size_t i, j, len;
    char low[MAX_FIELD_LEN];
    safe_strcpy(low, s, sizeof(low));
    for (i = 0; low[i]; i++) low[i] = (char)tolower((unsigned char)low[i]);
    char *f = strstr(low, "...check the file:");
    if (f) s[f - low] = '\0';
    len = strlen(s);
    for (i = 0; i < len; ) {
        if (i + 1 < len && s[i] == '0' && s[i+1] == 'x') {
            j = i + 2;
            while (j < len && isxdigit((unsigned char)s[j])) j++;
            memmove(s+i, s+j, len-j+1); len -= (j-i);
        } else i++;
    }
    j = 0;
    for (i = 0; i < len; i++)
        if (!isdigit((unsigned char)s[i])) s[j++] = s[i];
    s[j] = '\0'; len = j;
    j = 0; int sp = 0;
    for (i = 0; i < len; i++) {
        if (isspace((unsigned char)s[i])) {
            if (!sp && j > 0) { s[j++] = ' '; sp = 1; }
        } else { s[j++] = (char)tolower((unsigned char)s[i]); sp = 0; }
    }
    if (j > 0 && s[j-1] == ' ') j--;
    s[j] = '\0';
}

static void make_key(const char *level, const char *msg,
                     const char *file, long line,
                     char *out, size_t n)
{
    char brief[MAX_FIELD_LEN], clean[MAX_FIELD_LEN], sev[32];
    derive_brief(msg ? msg : "", brief, sizeof(brief));
    safe_strcpy(clean, brief, sizeof(clean));
    clean_brief(clean);
    safe_strcpy(sev, level ? level : "UNKNOWN", sizeof(sev));
    for (int i = 0; sev[i]; i++) sev[i] = (char)toupper((unsigned char)sev[i]);
    snprintf(out, n, "%s|%s|%s|%ld", sev, clean, file ? file : "", line);
}

static int to_json(msgpack_object map, char *out, size_t n)
{
    size_t pos = 0;
    uint32_t i;
#define AP(fmt,...) do { int _r = snprintf(out+pos, n-pos, fmt, ##__VA_ARGS__); \
    if(_r<0||(size_t)_r>=n-pos) return -1; pos+=(size_t)_r; } while(0)
    AP("{");
    int first = 1;
    for (i = 0; i < map.via.map.size; i++) {
        msgpack_object k = map.via.map.ptr[i].key;
        msgpack_object v = map.via.map.ptr[i].val;
        if (k.type != MSGPACK_OBJECT_STR) continue;
        char ks[128] = {0};
        size_t kl = k.via.str.size < 127 ? k.via.str.size : 127;
        memcpy(ks, k.via.str.ptr, kl);
        if (!first) AP(",");
        first = 0;
        AP("\"%s\":", ks);
        switch (v.type) {
        case MSGPACK_OBJECT_STR: {
            char vs[MAX_FIELD_LEN] = {0};
            size_t vl = v.via.str.size < MAX_FIELD_LEN-1 ? v.via.str.size : MAX_FIELD_LEN-1;
            memcpy(vs, v.via.str.ptr, vl);
            AP("\"");
            for (size_t c = 0; c < vl; c++) {
                if      (vs[c]=='"')  AP("\\\"");
                else if (vs[c]=='\\') AP("\\\\");
                else if (vs[c]=='\n') AP("\\n");
                else AP("%c", vs[c]);
            }
            AP("\""); break;
        }
        case MSGPACK_OBJECT_POSITIVE_INTEGER: AP("%llu",(unsigned long long)v.via.u64); break;
        case MSGPACK_OBJECT_NEGATIVE_INTEGER: AP("%lld",(long long)v.via.i64); break;
        case MSGPACK_OBJECT_FLOAT32:
        case MSGPACK_OBJECT_FLOAT64:          AP("%g",v.via.f64); break;
        case MSGPACK_OBJECT_BOOLEAN:          AP("%s",v.via.boolean?"true":"false"); break;
        default:                              AP("null"); break;
        }
    }
    AP("}");
#undef AP
    return 0;
}

static int find_slot(struct batch_ctx *ctx, const char *key)
{
    for (int i = 0; i < ctx->record_count; i++)
        if (ctx->records[i].used && strcmp(ctx->records[i].alert_key, key) == 0)
            return i;
    return -1;
}

static void buf_add(struct batch_ctx *ctx, const char *key, const char *json)
{
    if (ctx->collapse_alerts) {
        int s = find_slot(ctx, key);
        if (s >= 0) { safe_strcpy(ctx->records[s].json, json, MAX_JSON_RECORD); return; }
    }
    if (ctx->record_count >= MAX_BATCH_SIZE) return;
    int idx = ctx->record_count++;
    ctx->records[idx].used = 1;
    safe_strcpy(ctx->records[idx].alert_key, key,  MAX_KEY_LEN);
    safe_strcpy(ctx->records[idx].json,      json, MAX_JSON_RECORD);
}

static int do_send(struct batch_ctx *ctx)
{
    if (ctx->record_count == 0) return 0;

    size_t cap = (size_t)ctx->record_count * MAX_JSON_RECORD + 32;
    char *payload = flb_malloc(cap);
    if (!payload) return -1;

    size_t pos = 0;
    payload[pos++] = '[';
    for (int i = 0; i < ctx->record_count; i++) {
        if (!ctx->records[i].used) continue;
        size_t jl = strlen(ctx->records[i].json);
        if (i > 0) payload[pos++] = ',';
        memcpy(payload + pos, ctx->records[i].json, jl);
        pos += jl;
    }
    payload[pos++] = ']';
    payload[pos]   = '\0';

    int success = 0;
    for (int attempt = 1; attempt <= ctx->retry_limit + 1; attempt++) {
        struct flb_upstream_conn *conn = flb_upstream_conn_get(ctx->upstream);
        if (!conn) {
            flb_error("[out_batchhttp] no upstream conn attempt %d", attempt);
            if (attempt <= ctx->retry_limit) sleep((unsigned)ctx->retry_delay_sec);
            continue;
        }
        struct flb_http_client *hc =
            flb_http_client(conn, FLB_HTTP_POST, ctx->path,
                            payload, pos, ctx->host, ctx->port, NULL, 0);
        flb_http_add_header(hc, "Content-Type", 12, "application/json", 16);
        int ret    = flb_http_do(hc, NULL);
        int status = hc->resp.status;
        flb_http_client_destroy(hc);
        flb_upstream_conn_release(conn);
        if (ret == 0 && status >= 200 && status < 300) {
            flb_info("[out_batchhttp] SENT %d record(s) -> %s:%d%s HTTP %d",
                     ctx->record_count, ctx->host, ctx->port, ctx->path, status);
            success = 1; break;
        }
        flb_warn("[out_batchhttp] FAILED attempt %d/%d ret=%d status=%d",
                 attempt, ctx->retry_limit+1, ret, status);
        if (attempt <= ctx->retry_limit) sleep((unsigned)ctx->retry_delay_sec);
    }
    if (!success)
        flb_error("[out_batchhttp] GIVING UP %d record(s) dropped", ctx->record_count);

    flb_free(payload);
    memset(ctx->records, 0, sizeof(struct buf_record) * (size_t)ctx->record_count);
    ctx->record_count = 0;
    ctx->oldest_ts    = 0;
    return success ? 0 : -1;
}

static int cb_init(struct flb_output_instance *ins,
                   struct flb_config *config, void *data)
{
    struct batch_ctx *ctx = flb_calloc(1, sizeof(struct batch_ctx));
    if (!ctx) { flb_errno(); return -1; }

    pthread_mutex_init(&ctx->lock, NULL);
    safe_strcpy(ctx->host, "127.0.0.1", MAX_HOST_LEN);
    ctx->port = 8080;
    safe_strcpy(ctx->path, "/", MAX_PATH_LEN);
    ctx->batch_size        = 200;
    ctx->batch_timeout_sec = 2;
    ctx->collapse_alerts   = 0;
    ctx->retry_limit       = 3;
    ctx->retry_delay_sec   = 1.0;

    const char *v;
    if ((v = flb_output_get_property("host",              ins))) safe_strcpy(ctx->host, v, MAX_HOST_LEN);
    if ((v = flb_output_get_property("port",              ins))) ctx->port              = atoi(v);
    if ((v = flb_output_get_property("path",              ins))) safe_strcpy(ctx->path, v, MAX_PATH_LEN);
    if ((v = flb_output_get_property("batch_size",        ins))) ctx->batch_size        = atoi(v);
    if ((v = flb_output_get_property("batch_timeout_sec", ins))) ctx->batch_timeout_sec = atoi(v);
    if ((v = flb_output_get_property("collapse_alerts",   ins)))
        ctx->collapse_alerts = (strcasecmp(v,"true")==0||strcmp(v,"1")==0) ? 1 : 0;
    if ((v = flb_output_get_property("retry_limit",       ins))) ctx->retry_limit       = atoi(v);
    if ((v = flb_output_get_property("retry_delay_sec",   ins))) ctx->retry_delay_sec   = atof(v);

    ctx->upstream = flb_upstream_create(config, ctx->host, ctx->port,
                                        FLB_IO_TCP, NULL);
    if (!ctx->upstream) {
        flb_error("[out_batchhttp] upstream create failed %s:%d",
                  ctx->host, ctx->port);
        flb_free(ctx);
        return -1;
    }
    /* Allow async-unsafe access from our mutex-protected flush */
    flb_stream_disable_async_mode(&ctx->upstream->base);

    flb_output_set_context(ins, ctx);
    flb_info("[out_batchhttp] init host=%s port=%d batch=%d timeout=%ds collapse=%s",
             ctx->host, ctx->port, ctx->batch_size, ctx->batch_timeout_sec,
             ctx->collapse_alerts ? "true" : "false");
    return 0;
}

static void cb_flush(struct flb_event_chunk *event_chunk,
                     struct flb_output_flush *out_flush,
                     struct flb_input_instance *i_ins,
                     void *out_context,
                     struct flb_config *config)
{
    struct batch_ctx *ctx = out_context;
    const void *data  = event_chunk->data;
    size_t      bytes = event_chunk->size;

    msgpack_unpacked result;
    size_t off = 0;
    msgpack_unpacked_init(&result);

    pthread_mutex_lock(&ctx->lock);

    while (msgpack_unpack_next(&result, data, bytes, &off)
           == MSGPACK_UNPACK_SUCCESS) {

        msgpack_object root = result.data;
        if (root.type != MSGPACK_OBJECT_ARRAY || root.via.array.size != 2)
            continue;

        msgpack_object map = root.via.array.ptr[1];
        if (map.type != MSGPACK_OBJECT_MAP) continue;

        const char *lv=NULL, *msg=NULL, *fi=NULL;
        size_t lv_l=0, msg_l=0, fi_l=0;
        long line_val = 0;
        uint32_t i;

        for (i = 0; i < map.via.map.size; i++) {
            msgpack_object k = map.via.map.ptr[i].key;
            msgpack_object v = map.via.map.ptr[i].val;
            if (k.type != MSGPACK_OBJECT_STR) continue;
#define KIS(n) (k.via.str.size==strlen(n)&&strncmp(k.via.str.ptr,n,k.via.str.size)==0)
            if (KIS("level")  &&v.type==MSGPACK_OBJECT_STR){lv =v.via.str.ptr;lv_l =v.via.str.size;}
            if (KIS("message")&&v.type==MSGPACK_OBJECT_STR){msg=v.via.str.ptr;msg_l=v.via.str.size;}
            if (KIS("file")   &&v.type==MSGPACK_OBJECT_STR){fi =v.via.str.ptr;fi_l =v.via.str.size;}
            if (KIS("line")) {
                if (v.type==MSGPACK_OBJECT_POSITIVE_INTEGER) line_val=(long)v.via.u64;
                else if (v.type==MSGPACK_OBJECT_NEGATIVE_INTEGER) line_val=(long)v.via.i64;
            }
#undef KIS
        }

        char lv_b[64]={0}, msg_b[MAX_FIELD_LEN]={0}, fi_b[MAX_FIELD_LEN]={0};
        if (lv)  memcpy(lv_b,  lv,  lv_l  < 63           ? lv_l  : 63);
        if (msg) memcpy(msg_b, msg, msg_l < MAX_FIELD_LEN-1 ? msg_l : MAX_FIELD_LEN-1);
        if (fi)  memcpy(fi_b,  fi,  fi_l  < MAX_FIELD_LEN-1 ? fi_l  : MAX_FIELD_LEN-1);

        char key[MAX_KEY_LEN] = {0};
        if (ctx->collapse_alerts)
            make_key(lv_b, msg_b, fi_b, line_val, key, MAX_KEY_LEN);

        char json[MAX_JSON_RECORD];
        if (to_json(map, json, sizeof(json)) != 0) continue;

        if (ctx->record_count == 0) ctx->oldest_ts = time(NULL);
        buf_add(ctx, key, json);

        double age = ctx->oldest_ts > 0 ? difftime(time(NULL), ctx->oldest_ts) : 0.0;
        if (ctx->record_count >= ctx->batch_size) {
            flb_info("[out_batchhttp] count flush: %d records", ctx->record_count);
            do_send(ctx);
        } else if (ctx->oldest_ts > 0 && age >= ctx->batch_timeout_sec) {
            flb_info("[out_batchhttp] timeout flush: %d records age=%.0fs",
                     ctx->record_count, age);
            do_send(ctx);
        }
    }

    msgpack_unpacked_destroy(&result);
    pthread_mutex_unlock(&ctx->lock);
    FLB_OUTPUT_RETURN(FLB_OK);
}

static int cb_exit(void *data, struct flb_config *config)
{
    struct batch_ctx *ctx = data;
    if (!ctx) return 0;
    pthread_mutex_lock(&ctx->lock);
    if (ctx->record_count > 0) {
        flb_info("[out_batchhttp] shutdown flush: %d records", ctx->record_count);
        do_send(ctx);
    }
    pthread_mutex_unlock(&ctx->lock);
    pthread_mutex_destroy(&ctx->lock);
    if (ctx->upstream) flb_upstream_destroy(ctx->upstream);
    flb_free(ctx);
    return 0;
}

struct flb_output_plugin out_batchhttp_plugin = {
    .name        = "batch_http",
    .description = "Batching and alert-collapsing HTTP output",
    .cb_init     = cb_init,
    .cb_flush    = cb_flush,
    .cb_exit     = cb_exit,
    .flags       = 0
};
