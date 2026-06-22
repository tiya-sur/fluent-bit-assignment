
#include <fluent-bit/flb_filter.h>
#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <msgpack.h>
#include <string.h>
#include <stdlib.h>
#include <regex.h>

#define MAX_KEY_LEN   64
#define MAX_PATH_LEN  512
#define MAX_REGEX_LEN 256

struct stratid_ctx {
    char    source_key[MAX_KEY_LEN];
    char    target_key[MAX_KEY_LEN];
    char    regex_str[MAX_REGEX_LEN];
    regex_t re;
    int     re_compiled;
    struct flb_filter_instance *f_ins;
};

/* ------------------------------------------------------------------ helpers */

static void safe_strcpy(char *dst, const char *src, size_t n)
{
    if (!src) { dst[0] = '\0'; return; }
    strncpy(dst, src, n - 1);
    dst[n - 1] = '\0';
}

/*
 * Return a pointer to the basename within path (no allocation, no mutation).
 * Points past the last '/' or to path itself if no slash present.
 */
static const char *basename_of(const char *path)
{
    const char *p = strrchr(path, '/');
    return p ? p + 1 : path;
}

/*
 * Run the compiled regex against `filename`.
 * Returns the integer value of the first capture group, or -1 on no match.
 */
static long extract_id(struct stratid_ctx *ctx, const char *filename)
{
    regmatch_t pmatch[2]; /* [0] whole match, [1] first capture group */

    if (regexec(&ctx->re, filename, 2, pmatch, 0) != 0)
        return -1;

    if (pmatch[1].rm_so < 0)
        return -1;

    size_t len = (size_t)(pmatch[1].rm_eo - pmatch[1].rm_so);
    if (len == 0 || len > 20)
        return -1;

    char num[24] = {0};
    memcpy(num, filename + pmatch[1].rm_so, len);
    return atol(num);
}

/* ---------------------------------------------------------------- callbacks */

static int cb_stratid_init(struct flb_filter_instance *f_ins,
                            struct flb_config *config,
                            void *data)
{
    (void)config; (void)data;

    struct stratid_ctx *ctx = flb_calloc(1, sizeof(struct stratid_ctx));
    if (!ctx) { flb_errno(); return -1; }

    ctx->f_ins = f_ins;

    /* defaults */
    safe_strcpy(ctx->source_key, "source_file",  sizeof(ctx->source_key));
    safe_strcpy(ctx->target_key, "strat_id",     sizeof(ctx->target_key));
    safe_strcpy(ctx->regex_str,  "_([0-9]+)_",   sizeof(ctx->regex_str));

    const char *val;
    if ((val = flb_filter_get_property("source_key",     f_ins)))
        safe_strcpy(ctx->source_key, val, sizeof(ctx->source_key));
    if ((val = flb_filter_get_property("target_key",     f_ins)))
        safe_strcpy(ctx->target_key, val, sizeof(ctx->target_key));
    if ((val = flb_filter_get_property("filename_regex", f_ins)))
        safe_strcpy(ctx->regex_str,  val, sizeof(ctx->regex_str));

    int rc = regcomp(&ctx->re, ctx->regex_str, REG_EXTENDED);
    if (rc != 0) {
        char err[128];
        regerror(rc, &ctx->re, err, sizeof(err));
        flb_plg_error(f_ins, "bad regex '%s': %s", ctx->regex_str, err);
        flb_free(ctx);
        return -1;
    }
    ctx->re_compiled = 1;

    flb_filter_set_context(f_ins, ctx);
    flb_plg_info(f_ins,
                 "initialized. source_key='%s' target_key='%s' regex='%s'",
                 ctx->source_key, ctx->target_key, ctx->regex_str);
    return 0;
}

static int cb_stratid_filter(const void *data, size_t bytes,
                              const char *tag, int tag_len,
                              void **out_buf, size_t *out_size,
                              struct flb_filter_instance *f_ins,
                              struct flb_input_instance *i_ins,
                              void *filter_context,
                              struct flb_config *config)
{
    (void)tag; (void)tag_len; (void)f_ins; (void)i_ins; (void)config;

    struct stratid_ctx *ctx = filter_context;
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

        /* Pass through anything that isn't a [timestamp, map] pair */
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

        /* Find the configured source_key inside the map */
        const char *src_val = NULL;
        size_t      src_len = 0;

        for (uint32_t i = 0; i < map.via.map.size; i++) {
            msgpack_object k = map.via.map.ptr[i].key;
            msgpack_object v = map.via.map.ptr[i].val;

            if (k.type != MSGPACK_OBJECT_STR) continue;
            if (v.type != MSGPACK_OBJECT_STR) continue;

            if (k.via.str.size == strlen(ctx->source_key) &&
                strncmp(k.via.str.ptr, ctx->source_key,
                        k.via.str.size) == 0) {
                src_val = v.via.str.ptr;
                src_len = v.via.str.size;
                break;
            }
        }

        /* Extract the ID from the basename */
        long strat_id = -1;
        if (src_val && src_len > 0) {
            /* Copy to a null-terminated buffer so regex works correctly */
            char path_buf[MAX_PATH_LEN] = {0};
            size_t copy_len = src_len < MAX_PATH_LEN - 1
                              ? src_len : MAX_PATH_LEN - 1;
            memcpy(path_buf, src_val, copy_len);

            strat_id = extract_id(ctx, basename_of(path_buf));
        }

        /* Repack: original fields + strat_id (only when match found) */
        msgpack_pack_array(&pck, 2);
        msgpack_pack_object(&pck, ts);

        if (strat_id >= 0) {
            /* One extra field */
            msgpack_pack_map(&pck, map.via.map.size + 1);
            for (uint32_t i = 0; i < map.via.map.size; i++) {
                msgpack_pack_object(&pck, map.via.map.ptr[i].key);
                msgpack_pack_object(&pck, map.via.map.ptr[i].val);
            }
            size_t klen = strlen(ctx->target_key);
            msgpack_pack_str(&pck, klen);
            msgpack_pack_str_body(&pck, ctx->target_key, klen);
            msgpack_pack_int64(&pck, (int64_t)strat_id);
        } else {
            /* No ID found — pass record through unchanged */
            msgpack_pack_map(&pck, map.via.map.size);
            for (uint32_t i = 0; i < map.via.map.size; i++) {
                msgpack_pack_object(&pck, map.via.map.ptr[i].key);
                msgpack_pack_object(&pck, map.via.map.ptr[i].val);
            }
        }
    }

    msgpack_unpacked_destroy(&result);
    *out_buf  = sbuf.data;
    *out_size = sbuf.size;
    return FLB_FILTER_MODIFIED;
}

static int cb_stratid_exit(void *data, struct flb_config *config)
{
    (void)config;
    struct stratid_ctx *ctx = data;
    if (ctx) {
        if (ctx->re_compiled) regfree(&ctx->re);
        flb_free(ctx);
    }
    return 0;
}

struct flb_filter_plugin filter_stratid_plugin = {
    .name        = "stratid_filter",
    .description = "Extract integer strategy ID from log filename via configurable regex",
    .cb_init     = cb_stratid_init,
    .cb_filter   = cb_stratid_filter,
    .cb_exit     = cb_stratid_exit,
    .flags       = 0
};
