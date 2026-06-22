
#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <msgpack.h>
#include <regex.h>
#include <string.h>
#include <stdlib.h>

struct extract_id_ctx {
    regex_t  regex;
    char    *output_key;
    char    *filename_key;
};

static int cb_init(struct flb_filter_instance *f_ins,
                   struct flb_config *config, void *data)
{
    struct extract_id_ctx *ctx;
    const char *pattern, *output_key, *filename_key;

    ctx = flb_calloc(1, sizeof(struct extract_id_ctx));
    if (!ctx) return -1;

    pattern = flb_filter_get_property("file_pattern", f_ins);
    if (!pattern) {
        flb_error("[extract_id] 'file_pattern' not specified");
        flb_free(ctx);
        return -1;
    }

    output_key   = flb_filter_get_property("output_key",   f_ins);
    filename_key = flb_filter_get_property("filename_key", f_ins);
    ctx->output_key   = flb_strdup(output_key   ? output_key   : "strat_id");
    ctx->filename_key = flb_strdup(filename_key ? filename_key : "source_file");

    if (regcomp(&ctx->regex, pattern, REG_EXTENDED) != 0) {
        flb_error("[extract_id] failed to compile regex: %s", pattern);
        flb_free(ctx->output_key);
        flb_free(ctx->filename_key);
        flb_free(ctx);
        return -1;
    }

    flb_filter_set_context(f_ins, ctx);
    flb_plg_info(f_ins, "initialized. filename_key='%s' output_key='%s' pattern='%s'",
                 ctx->filename_key, ctx->output_key, pattern);
    return 0;
}

static int cb_filter(const void *data, size_t bytes,
                     const char *tag, int tag_len,
                     void **out_data, size_t *out_size,
                     struct flb_filter_instance *f_ins,
                     struct flb_input_instance *i_ins,
                     void *context, struct flb_config *config)
{
    struct extract_id_ctx *ctx = context;
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

        /* Find filename_key value */
        const char *filename = NULL;
        size_t      fname_len = 0;
        for (uint32_t i = 0; i < map.via.map.size; i++) {
            msgpack_object k = map.via.map.ptr[i].key;
            msgpack_object v = map.via.map.ptr[i].val;
            if (k.type != MSGPACK_OBJECT_STR || v.type != MSGPACK_OBJECT_STR) continue;
            if (k.via.str.size == strlen(ctx->filename_key) &&
                strncmp(k.via.str.ptr, ctx->filename_key, k.via.str.size) == 0) {
                filename  = v.via.str.ptr;
                fname_len = v.via.str.size;
                break;
            }
        }

        /* Extract ID from basename */
        long    extracted_id = -1;
        int     found_id     = 0;
        if (filename && fname_len > 0) {
            char fname_copy[512] = {0};
            size_t copy_len = fname_len < 511 ? fname_len : 511;
            memcpy(fname_copy, filename, copy_len);

            /* Use basename only */
            const char *base = strrchr(fname_copy, '/');
            base = base ? base + 1 : fname_copy;

            regmatch_t matches[2];
            if (regexec(&ctx->regex, base, 2, matches, 0) == 0 &&
                matches[1].rm_so >= 0) {
                int id_len = matches[1].rm_eo - matches[1].rm_so;
                char id_str[32] = {0};
                if (id_len > 0 && id_len < (int)sizeof(id_str)) {
                    memcpy(id_str, base + matches[1].rm_so, id_len);
                    extracted_id = atol(id_str);
                    found_id = 1;
                }
            }
        }

        /* Repack with optional extra field */
        msgpack_pack_array(&pck, 2);
        msgpack_pack_object(&pck, ts);
        msgpack_pack_map(&pck, map.via.map.size + (found_id ? 1 : 0));

        for (uint32_t i = 0; i < map.via.map.size; i++) {
            msgpack_pack_object(&pck, map.via.map.ptr[i].key);
            msgpack_pack_object(&pck, map.via.map.ptr[i].val);
        }
        if (found_id) {
            size_t klen = strlen(ctx->output_key);
            msgpack_pack_str(&pck, klen);
            msgpack_pack_str_body(&pck, ctx->output_key, klen);
            msgpack_pack_int64(&pck, (int64_t)extracted_id);
        }
    }

    msgpack_unpacked_destroy(&result);
    *out_data = sbuf.data;
    *out_size = sbuf.size;
    return FLB_FILTER_MODIFIED;
}

static int cb_exit(void *data, struct flb_config *config)
{
    struct extract_id_ctx *ctx = data;
    if (ctx) {
        regfree(&ctx->regex);
        flb_free(ctx->output_key);
        flb_free(ctx->filename_key);
        flb_free(ctx);
    }
    return 0;
}

struct flb_filter_plugin filter_extract_id_plugin = {
    .name        = "extract_id",
    .description = "Extract numeric ID from log filename",
    .cb_init     = cb_init,
    .cb_filter   = cb_filter,
    .cb_exit     = cb_exit,
    .flags       = 0
};
PLUGIN_EOF
