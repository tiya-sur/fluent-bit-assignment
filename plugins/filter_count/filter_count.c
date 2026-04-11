#include <fluent-bit/flb_filter.h>
#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <msgpack.h>
#include <string.h>
#include <strings.h>

struct count_ctx {
    int count_debug;
    int count_info;
    int count_warning;
    int count_error;
    int count_critical;
    int count_unknown;
    char output_key[64];
};

static int increment_and_get(struct count_ctx *ctx,
                              const char *level, size_t level_len)
{
    if (level && level_len == 5  && strncasecmp(level, "ERROR",    5) == 0)
        return ++ctx->count_error;
    if (level && level_len == 4  && strncasecmp(level, "INFO",     4) == 0)
        return ++ctx->count_info;
    if (level && level_len == 7  && strncasecmp(level, "WARNING",  7) == 0)
        return ++ctx->count_warning;
    if (level && level_len == 5  && strncasecmp(level, "DEBUG",    5) == 0)
        return ++ctx->count_debug;
    if (level && level_len == 8  && strncasecmp(level, "CRITICAL", 8) == 0)
        return ++ctx->count_critical;
    return ++ctx->count_unknown;
}

static int cb_count_init(struct flb_filter_instance *f_ins,
                         struct flb_config *config,
                         void *data)
{
    struct count_ctx *ctx;
    const char *val;

    ctx = flb_calloc(1, sizeof(struct count_ctx));
    if (!ctx) {
        flb_errno();
        return -1;
    }

    strncpy(ctx->output_key, "count", sizeof(ctx->output_key) - 1);

    val = flb_filter_get_property("output_key", f_ins);
    if (val) {
        strncpy(ctx->output_key, val, sizeof(ctx->output_key) - 1);
    }

    flb_filter_set_context(f_ins, ctx);
    flb_plg_info(f_ins, "initialized. output_key=\"%s\"", ctx->output_key);
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
    msgpack_sbuffer sbuf;
    msgpack_packer  pck;
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

        const char *level_str = NULL;
        size_t      level_len = 0;
        uint32_t i;

        for (i = 0; i < map.via.map.size; i++) {
            msgpack_object k = map.via.map.ptr[i].key;
            msgpack_object v = map.via.map.ptr[i].val;

            if (k.type != MSGPACK_OBJECT_STR)
                continue;

            if (k.via.str.size == 5 &&
                strncmp(k.via.str.ptr, "level", 5) == 0 &&
                v.type == MSGPACK_OBJECT_STR)
            {
                level_str = v.via.str.ptr;
                level_len = v.via.str.size;
                break;
            }
        }

        int count = increment_and_get(ctx, level_str, level_len);

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
    if (ctx)
        flb_free(ctx);
    return 0;
}

struct flb_filter_plugin filter_count_plugin = {
    .name        = "count_filter",
    .description = "Add per-level running count field to each log record",
    .cb_init     = cb_count_init,
    .cb_filter   = cb_count_filter,
    .cb_exit     = cb_count_exit,
    .flags       = 0
};
