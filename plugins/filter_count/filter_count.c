#include <fluent-bit/flb_filter.h>
#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <msgpack.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdio.h>

#define MAX_KEY_LEN    512
#define MAX_FIELD_LEN  1024
#define MAX_COUNTERS   8192

struct counter_entry {
    char key[MAX_KEY_LEN];
    int  count;
};

struct count_ctx {
    struct counter_entry *entries;
    int    entry_count;
    int    entry_cap;
    char   output_key[64];
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
        memcpy(out, msg, l);
        out[l] = '\0';
    } else {
        safe_strcpy(out, msg, n);
    }
}

static void clean_brief(char *s)
{
    size_t i, j, len;
    char low[MAX_FIELD_LEN];
    safe_strcpy(low, s, sizeof(low));
    for (i = 0; low[i]; i++)
        low[i] = (char)tolower((unsigned char)low[i]);
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

static void make_alert_key(const char *level, const char *message,
                            const char *file, long line,
                            char *out, size_t n)
{
    char brief[MAX_FIELD_LEN]   = {0};
    char cleaned[MAX_FIELD_LEN] = {0};
    char severity[32]           = {0};
    derive_brief(message ? message : "", brief, sizeof(brief));
    safe_strcpy(cleaned, brief, sizeof(cleaned));
    clean_brief(cleaned);
    safe_strcpy(severity, level ? level : "UNKNOWN", sizeof(severity));
    for (int i = 0; severity[i]; i++)
        severity[i] = (char)toupper((unsigned char)severity[i]);
    snprintf(out, n, "%s|%s|%s|%ld", severity, cleaned,
             file ? file : "", line);
}

static int counter_increment(struct count_ctx *ctx, const char *key)
{
    for (int i = 0; i < ctx->entry_count; i++) {
        if (strcmp(ctx->entries[i].key, key) == 0)
            return ++ctx->entries[i].count;
    }

    if (ctx->entry_count >= ctx->entry_cap) {
        int new_cap = ctx->entry_cap * 2;
        if (new_cap > MAX_COUNTERS) return -1;
        struct counter_entry *tmp = flb_realloc(
            ctx->entries,
            sizeof(struct counter_entry) * (size_t)new_cap);
        if (!tmp) return -1;
        ctx->entries    = tmp;
        ctx->entry_cap  = new_cap;
    }

    struct counter_entry *e = &ctx->entries[ctx->entry_count++];
    safe_strcpy(e->key, key, MAX_KEY_LEN);
    e->count = 1;
    return 1;
}

static int cb_count_init(struct flb_filter_instance *f_ins,
                         struct flb_config *config,
                         void *data)
{
    struct count_ctx *ctx = flb_calloc(1, sizeof(struct count_ctx));
    if (!ctx) { flb_errno(); return -1; }

    ctx->entry_cap = 256;
    ctx->entries   = flb_calloc((size_t)ctx->entry_cap,
                                sizeof(struct counter_entry));
    if (!ctx->entries) {
        flb_free(ctx);
        return -1;
    }

    safe_strcpy(ctx->output_key, "count", sizeof(ctx->output_key));

    const char *val = flb_filter_get_property("output_key", f_ins);
    if (val)
        safe_strcpy(ctx->output_key, val, sizeof(ctx->output_key));

    flb_filter_set_context(f_ins, ctx);
    flb_plg_info(f_ins,
                 "initialized. output_key=\"%s\" "
                 "counting by composite key "
                 "(severity|cleaned_brief|file|line)",
                 ctx->output_key);
    return 0;
}

static int cb_count_filter(const void *data, size_t bytes,
                            const char *tag, int tag_len,
                            void **out_buf, size_t *out_size,
                            struct flb_filter_instance *f_ins,
                            struct flb_input_instance *i_ins,
                            void *filter_context,
                            struct flb_config *config)
{
    struct count_ctx *ctx = filter_context;
    msgpack_unpacked result;
    msgpack_sbuffer  sbuf;
    msgpack_packer   pck;
    size_t off = 0;

    msgpack_sbuffer_init(&sbuf);
    msgpack_packer_init(&pck, &sbuf, msgpack_sbuffer_write);
    msgpack_unpacked_init(&result);

    while (msgpack_unpack_next(&result, data, bytes, &off)
           == MSGPACK_UNPACK_SUCCESS) {

        msgpack_object root = result.data;

        if (root.type != MSGPACK_OBJECT_ARRAY || root.via.array.size != 2) {
            msgpack_pack_object(&pck, root);
            continue;
        }

        msgpack_object ts  = root.via.array.ptr[0];
        msgpack_object map = root.via.array.ptr[1];

        if (map.type != MSGPACK_OBJECT_MAP) {
            msgpack_pack_object(&pck, root);
            continue;
        }

        const char *lv  = NULL, *msg = NULL, *fi = NULL;
        size_t      lv_l = 0,   msg_l = 0,   fi_l = 0;
        long        line_val = 0;
        uint32_t i;

        for (i = 0; i < map.via.map.size; i++) {
            msgpack_object k = map.via.map.ptr[i].key;
            msgpack_object v = map.via.map.ptr[i].val;
            if (k.type != MSGPACK_OBJECT_STR) continue;

#define KIS(n) (k.via.str.size == strlen(n) && \
                strncmp(k.via.str.ptr, (n), k.via.str.size) == 0)
            if (KIS("level") && v.type == MSGPACK_OBJECT_STR)
                { lv  = v.via.str.ptr; lv_l  = v.via.str.size; }
            if (KIS("message") && v.type == MSGPACK_OBJECT_STR)
                { msg = v.via.str.ptr; msg_l = v.via.str.size; }
            if (KIS("file") && v.type == MSGPACK_OBJECT_STR)
                { fi  = v.via.str.ptr; fi_l  = v.via.str.size; }
            if (KIS("line")) {
                if (v.type == MSGPACK_OBJECT_POSITIVE_INTEGER)
                    line_val = (long)v.via.u64;
                else if (v.type == MSGPACK_OBJECT_NEGATIVE_INTEGER)
                    line_val = (long)v.via.i64;
            }
#undef KIS
        }

        char lv_b[64]           = {0};
        char msg_b[MAX_FIELD_LEN] = {0};
        char fi_b[MAX_FIELD_LEN]  = {0};

        if (lv)  memcpy(lv_b,  lv,  lv_l  < 63            ? lv_l  : 63);
        if (msg) memcpy(msg_b, msg, msg_l < MAX_FIELD_LEN-1 ? msg_l : MAX_FIELD_LEN-1);
        if (fi)  memcpy(fi_b,  fi,  fi_l  < MAX_FIELD_LEN-1 ? fi_l  : MAX_FIELD_LEN-1);

        char alert_key[MAX_KEY_LEN] = {0};
        make_alert_key(lv_b, msg_b, fi_b, line_val,
                       alert_key, MAX_KEY_LEN);

        int count = counter_increment(ctx, alert_key);
        if (count < 0) count = 0;

        msgpack_pack_array(&pck, 2);
        msgpack_pack_object(&pck, ts);
        msgpack_pack_map(&pck, map.via.map.size + 1);

        for (i = 0; i < map.via.map.size; i++) {
            msgpack_pack_object(&pck, map.via.map.ptr[i].key);
            msgpack_pack_object(&pck, map.via.map.ptr[i].val);
        }

        size_t klen = strlen(ctx->output_key);
        msgpack_pack_str(&pck, klen);
        msgpack_pack_str_body(&pck, ctx->output_key, klen);
        msgpack_pack_int(&pck, count);
    }

    msgpack_unpacked_destroy(&result);
    *out_buf  = sbuf.data;
    *out_size = sbuf.size;
    return FLB_FILTER_MODIFIED;
}

static int cb_count_exit(void *data, struct flb_config *config)
{
    struct count_ctx *ctx = data;
    if (ctx) {
        if (ctx->entries) flb_free(ctx->entries);
        flb_free(ctx);
    }
    return 0;
}

struct flb_filter_plugin filter_count_plugin = {
    .name        = "count_filter",
    .description = "Add composite-key alert count field to each log record",
    .cb_init     = cb_count_init,
    .cb_filter   = cb_count_filter,
    .cb_exit     = cb_count_exit,
    .flags       = 0
};
