#include <fluent-bit/flb_filter_plugin.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <msgpack.h>
#include <regex.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

struct extract_id_ctx {
    regex_t regex;
    char *output_key;
    char *filename_key;  /* Field name containing filename */
};

static int cb_init(struct flb_filter_instance *f_ins,
                   struct flb_config *config,
                   void *data)
{
    struct extract_id_ctx *ctx;
    const char *pattern;
    const char *output_key;
    const char *filename_key;
    int ret;

    ctx = flb_calloc(1, sizeof(struct extract_id_ctx));
    if (!ctx) {
        flb_error("[extract_id] memory allocation failed");
        return -1;
    }

    /* Get regex pattern from config */
    pattern = flb_filter_get_property("file_pattern", f_ins);
    if (!pattern) {
        flb_error("[extract_id] 'file_pattern' not specified");
        flb_free(ctx);
        return -1;
    }

    /* Get output key name (default: strat_id) */
    output_key = flb_filter_get_property("output_key", f_ins);
    if (!output_key) {
        output_key = "strat_id";
    }
    ctx->output_key = flb_strdup(output_key);

    /* Get filename key (default: _filename or filename from tail input) */
    filename_key = flb_filter_get_property("filename_key", f_ins);
    if (!filename_key) {
        filename_key = "_filename";
    }
    ctx->filename_key = flb_strdup(filename_key);

    /* Compile regex */
    ret = regcomp(&ctx->regex, pattern, REG_EXTENDED);
    if (ret != 0) {
        char errbuf[256];
        regerror(ret, &ctx->regex, errbuf, sizeof(errbuf));
        flb_error("[extract_id] regex compilation failed: %s", errbuf);
        flb_free(ctx->output_key);
        flb_free(ctx->filename_key);
        flb_free(ctx);
        return -1;
    }

    flb_filter_set_context(f_ins, ctx);
    flb_info("[extract_id] plugin initialized with pattern: %s, output_key: %s, filename_key: %s",
             pattern, output_key, filename_key);

    return 0;
}

static int cb_filter(const void *data, size_t bytes,
                     const char *tag, int tag_len,
                     void **out_data, size_t *out_size,
                     struct flb_filter_instance *f_ins,
                     void *context,
                     struct flb_config *config)
{
    struct extract_id_ctx *ctx = context;
    msgpack_unpacked result;
    msgpack_packer packer;
    msgpack_object_array *arr;
    msgpack_object_map *map;
    size_t off = 0;
    int i;
    int ret;
    int found_id = 0;
    long extracted_id = 0;
    const char *filename = NULL;
    size_t filename_len = 0;
    regmatch_t matches[2];
    char id_str[32];
    char filename_copy[512];
    char *buf;

    /* Output buffer */
    buf = flb_malloc(bytes + 256);
    if (!buf) {
        return FLB_FILTER_NOOP;
    }

    msgpack_packer_init(&packer, buf, bytes + 256);
    msgpack_unpacked_init(&result);

    /* Process each record */
    while (msgpack_unpack_next(&result, data, bytes, &off) == MSGPACK_UNPACK_SUCCESS) {
        if (result.obj.type != MSGPACK_OBJECT_ARRAY || result.obj.via.array.size != 2) {
            msgpack_pack_object(&packer, result.obj);
            continue;
        }

        arr = &result.obj.via.array;
        msgpack_object timestamp = arr->ptr[0];
        msgpack_object record = arr->ptr[1];

        if (record.type != MSGPACK_OBJECT_MAP) {
            msgpack_pack_object(&packer, result.obj);
            continue;
        }

        map = &record.via.map;
        found_id = 0;
        extracted_id = 0;
        filename = NULL;
        filename_len = 0;

        /* Search for filename field in the map */
        for (i = 0; i < map->size; i++) {
            msgpack_object key = map->ptr[i].key;
            msgpack_object val = map->ptr[i].val;

            if (key.type == MSGPACK_OBJECT_STR) {
                /* Check if this is the filename field */
                if (strncmp(key.via.str.ptr, ctx->filename_key, key.via.str.size) == 0) {
                    if (val.type == MSGPACK_OBJECT_STR) {
                        filename = val.via.str.ptr;
                        filename_len = val.via.str.size;
                    }
                }
            }
        }

        /* If filename found, try to extract ID using regex */
        if (filename && filename_len > 0) {
            if (filename_len >= sizeof(filename_copy)) {
                filename_len = sizeof(filename_copy) - 1;
            }
            strncpy(filename_copy, filename, filename_len);
            filename_copy[filename_len] = '\0';

            /* Match regex and extract ID from first capture group */
            if (regexec(&ctx->regex, filename_copy, 2, matches, 0) == 0) {
                if (matches[1].rm_so != -1) {
                    int id_len = matches[1].rm_eo - matches[1].rm_so;
                    if (id_len > 0 && id_len < (int)sizeof(id_str)) {
                        strncpy(id_str, filename_copy + matches[1].rm_so, id_len);
                        id_str[id_len] = '\0';
                        extracted_id = atol(id_str);
                        found_id = 1;
                    }
                }
            }
        }

        /* Pack timestamp and enriched record */
        msgpack_pack_array(&packer, 2);
        msgpack_pack_object(&packer, timestamp);

        /* Pack map with all original fields + new ID field */
        int new_map_size = map->size + (found_id ? 1 : 0);
        msgpack_pack_map(&packer, new_map_size);

        /* Pack all original fields */
        for (i = 0; i < map->size; i++) {
            msgpack_pack_object(&packer, map->ptr[i].key);
            msgpack_pack_object(&packer, map->ptr[i].val);
        }

        /* Add the extracted ID if found */
        if (found_id) {
            msgpack_pack_str(&packer, strlen(ctx->output_key));
            msgpack_pack_str_body(&packer, ctx->output_key, strlen(ctx->output_key));
            msgpack_pack_int64(&packer, extracted_id);
        }
    }

    msgpack_unpacked_destroy(&result);

    *out_data = buf;
    *out_size = msgpack_packer_buffered_size(&packer);

    return FLB_FILTER_OK;
}

static int cb_exit(void *data, struct flb_config *config)
{
    struct extract_id_ctx *ctx = data;
    if (!ctx) {
        return 0;
    }

    regfree(&ctx->regex);
    if (ctx->output_key) {
        flb_free(ctx->output_key);
    }
    if (ctx->filename_key) {
        flb_free(ctx->filename_key);
    }
    flb_free(ctx);
    return 0;
}

struct flb_filter_plugin filter_extract_id_plugin = {
    .name         = "extract_id",
    .description  = "Extract numeric ID from log filename and add to record",
    .cb_init      = cb_init,
    .cb_filter    = cb_filter,
    .cb_exit      = cb_exit,
    .flags        = 0
};
