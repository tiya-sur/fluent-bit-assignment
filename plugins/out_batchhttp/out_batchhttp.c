#include <fluent-bit/flb_output_plugin.h>
#include <fluent-bit/flb_http_client.h>
#include <fluent-bit/flb_upstream.h>
#include <fluent-bit/flb_upstream_conn.h>
#include <fluent-bit/flb_mem.h>
#include <fluent-bit/flb_log.h>
#include <fluent-bit/flb_time.h>
#include <msgpack.h>
#include <string.h>
#include <strings.h>
#include <stdlib.h>
#include <stdio.h>
#include <ctype.h>
#include <unistd.h>
#include <pthread.h>
#include <time.h>

#define MAX_BATCH_SIZE  512
#define MAX_KEY_LEN     256
#define MAX_FIELD_LEN   512
#define MAX_HOST_LEN    128
#define MAX_PATH_LEN    256
#define MAX_JSON_REC    2048
#define MAX_SEEN_KEYS   8192

struct buf_record {
    char *alert_key;
    char *json;
};

struct seen_keys {
    char **keys;
    int count;
    int cap;
};

struct batch_ctx {
    char   host[MAX_HOST_LEN];
    int    port;
    char   path[MAX_PATH_LEN];
    int    batch_size;
    int    batch_timeout_sec;
    int    collapse_alerts;
    int    retry_limit;
    int    retry_delay_sec;
    struct buf_record **records;
    int    record_count;
    time_t oldest_ts;
    struct seen_keys seen;
    pthread_mutex_t      lock;
    struct flb_upstream *upstream;
    struct flb_output_instance *ins;
};

static void safe_strcpy(char *dst, const char *src, size_t n)
{
    if (!src) { dst[0]='\0'; return; }
    strncpy(dst, src, n-1);
    dst[n-1]='\0';
}

static int seen_contains(struct seen_keys *sk, const char *key)
{
    for (int i=0; i<sk->count; i++)
        if (strcmp(sk->keys[i], key)==0) return 1;
    return 0;
}

static int seen_add(struct seen_keys *sk, const char *key)
{
    if (sk->count >= sk->cap) {
        int nc = sk->cap * 2;
        if (nc > MAX_SEEN_KEYS) return -1;
        char **tmp = flb_realloc(sk->keys, sizeof(char *) * (size_t)nc);
        if (!tmp) return -1;
        sk->keys = tmp;
        sk->cap = nc;
    }
    sk->keys[sk->count] = flb_strdup(key);
    if (!sk->keys[sk->count]) return -1;
    sk->count++;
    return 0;
}

static void derive_brief(const char *msg, char *out, size_t n)
{
    const char *sep = strstr(msg, ";;;");
    if (sep) {
        size_t len = (size_t)(sep - msg);
        if (len >= n) len = n-1;
        memcpy(out, msg, len);
        out[len] = '\0';
    } else {
        safe_strcpy(out, msg, n);
    }
}

static void clean_brief(char *s)
{
    size_t i, j, len = strlen(s);
    char *lc = flb_malloc(len + 1);
    if (!lc) return;
    memcpy(lc, s, len + 1);
    for (i=0; lc[i]; i++) lc[i]=(char)tolower((unsigned char)lc[i]);
    char *found = strstr(lc, "...check the file:");
    if (found) { s[(size_t)(found-lc)]='\0'; len=strlen(s); }
    flb_free(lc);

    for (i=0; i<len; ) {
        if (s[i]=='0' && i+1<len && s[i+1]=='x') {
            j=i+2;
            while (j<len && isxdigit((unsigned char)s[j])) j++;
            memmove(s+i, s+j, len-j+1); len-=(j-i);
        } else i++;
    }
    j=0;
    for (i=0; i<len; i++) if (!isdigit((unsigned char)s[i])) s[j++]=s[i];
    s[j]='\0'; len=j;
    j=0; int sp=0;
    for (i=0; i<len; i++) {
        if (isspace((unsigned char)s[i])) { if(!sp&&j>0){s[j++]=' ';sp=1;} }
        else { s[j++]=(char)tolower((unsigned char)s[i]); sp=0; }
    }
    if (j>0 && s[j-1]==' ') j--;
    s[j]='\0';
}

static char *make_alert_key(const char *level, const char *msg,
                             const char *file, long line)
{
    char *brief   = flb_malloc(MAX_FIELD_LEN);
    char *cleaned = flb_malloc(MAX_FIELD_LEN);
    char *out     = flb_malloc(MAX_KEY_LEN);
    if (!brief || !cleaned || !out) {
        flb_free(brief); flb_free(cleaned); flb_free(out);
        return NULL;
    }
    derive_brief(msg ? msg : "", brief, MAX_FIELD_LEN);
    safe_strcpy(cleaned, brief, MAX_FIELD_LEN);
    flb_free(brief);
    clean_brief(cleaned);

    char sev[32];
    safe_strcpy(sev, level ? level : "UNKNOWN", sizeof(sev));
    for (int i=0; sev[i]; i++) sev[i]=(char)toupper((unsigned char)sev[i]);
    snprintf(out, MAX_KEY_LEN, "%s|%s|%s|%ld", sev, cleaned, file ? file : "", line);
    flb_free(cleaned);
    return out;
}

static int map_to_json(msgpack_object map, char *out, size_t out_n)
{
    char *tmp = flb_malloc(out_n);
    if (!tmp) return -1;
    size_t pos=0;

#define AP(fmt, ...) \
    do { int _n=snprintf(tmp+pos,out_n-pos,fmt,##__VA_ARGS__); \
         if(_n<0||(size_t)_n>=out_n-pos){flb_free(tmp);return -1;} \
         pos+=(size_t)_n; } while(0)

    AP("{");
    int first=1;
    for (uint32_t i=0; i<map.via.map.size; i++) {
        msgpack_object k=map.via.map.ptr[i].key;
        msgpack_object v=map.via.map.ptr[i].val;
        if (k.type!=MSGPACK_OBJECT_STR) continue;
        char ks[128];
        size_t kl=k.via.str.size<127?k.via.str.size:127;
        memcpy(ks,k.via.str.ptr,kl); ks[kl]='\0';
        if (!first) AP(","); first=0;
        AP("\"%s\":",ks);
        switch(v.type) {
        case MSGPACK_OBJECT_STR: {
            size_t vl=v.via.str.size<MAX_FIELD_LEN-1?v.via.str.size:MAX_FIELD_LEN-1;
            AP("\"");
            for (size_t c=0;c<vl;c++) {
                char ch=v.via.str.ptr[c];
                if(ch=='"')       AP("\\\"");
                else if(ch=='\\') AP("\\\\");
                else if(ch=='\n') AP("\\n");
                else              AP("%c",ch);
            }
            AP("\""); break; }
        case MSGPACK_OBJECT_POSITIVE_INTEGER:
            AP("%llu",(unsigned long long)v.via.u64); break;
        case MSGPACK_OBJECT_NEGATIVE_INTEGER:
            AP("%lld",(long long)v.via.i64); break;
        case MSGPACK_OBJECT_FLOAT32:
        case MSGPACK_OBJECT_FLOAT64:
            AP("%g",v.via.f64); break;
        case MSGPACK_OBJECT_BOOLEAN:
            AP("%s",v.via.boolean?"true":"false"); break;
        default: AP("null"); break;
        }
    }
    AP("}");
#undef AP
    safe_strcpy(out, tmp, out_n);
    flb_free(tmp);
    return 0;
}

static void buf_free_all(struct batch_ctx *ctx)
{
    for (int i=0; i<ctx->record_count; i++) {
        if (ctx->records[i]) {
            flb_free(ctx->records[i]->alert_key);
            flb_free(ctx->records[i]->json);
            flb_free(ctx->records[i]);
            ctx->records[i]=NULL;
        }
    }
    ctx->record_count=0;
    ctx->oldest_ts=0;
}

static int find_slot(struct batch_ctx *ctx, const char *key)
{
    for (int i=0; i<ctx->record_count; i++)
        if (ctx->records[i] && strcmp(ctx->records[i]->alert_key,key)==0)
            return i;
    return -1;
}

static int buf_add(struct batch_ctx *ctx, const char *key, const char *json)
{
    if (ctx->collapse_alerts) {
        int s=find_slot(ctx,key);
        if (s>=0) {
            flb_free(ctx->records[s]->json);
            ctx->records[s]->json=flb_strdup(json);
            return 0;
        }
    }
    if (ctx->record_count>=ctx->batch_size) return 0;
    struct buf_record *r=flb_calloc(1,sizeof(struct buf_record));
    if (!r) return -1;
    r->alert_key=flb_strdup(key);
    r->json=flb_strdup(json);
    if (!r->alert_key||!r->json) {
        flb_free(r->alert_key); flb_free(r->json); flb_free(r); return -1;
    }
    ctx->records[ctx->record_count++]=r;
    return 0;
}

static int do_send_http(struct batch_ctx *ctx, const char *payload, size_t plen, int count)
{
    int success=0;
    for (int attempt=1; attempt<=ctx->retry_limit+1; attempt++) {
        struct flb_upstream_conn *u_conn=flb_upstream_conn_get(ctx->upstream);
        if (!u_conn) {
            flb_plg_warn(ctx->ins,"no upstream conn (attempt %d)",attempt);
            if (attempt<=ctx->retry_limit) sleep((unsigned)ctx->retry_delay_sec);
            continue;
        }
        struct flb_http_client *hc=
            flb_http_client(u_conn,FLB_HTTP_POST,ctx->path,
                            payload,plen,ctx->host,ctx->port,NULL,0);
        if (!hc) {
            flb_upstream_conn_release(u_conn);
            if (attempt<=ctx->retry_limit) sleep((unsigned)ctx->retry_delay_sec);
            continue;
        }
        flb_http_add_header(hc,"Content-Type",12,"application/json",16);
        size_t b_sent=0;
        int ret=flb_http_do(hc,&b_sent);
        int status=hc->resp.status;
        flb_http_client_destroy(hc);
        flb_upstream_conn_release(u_conn);
        if (ret==0&&status>=200&&status<300) {
            flb_plg_info(ctx->ins,"sent %d record(s) HTTP %d",count,status);
            success=1; break;
        }
        flb_plg_warn(ctx->ins,"attempt %d/%d failed ret=%d status=%d",
                     attempt,ctx->retry_limit+1,ret,status);
        if (attempt<=ctx->retry_limit) sleep((unsigned)ctx->retry_delay_sec);
    }
    if (!success)
        flb_plg_error(ctx->ins,"gave up after %d attempts, %d record(s) dropped",
                      ctx->retry_limit+1,count);
    return success?0:-1;
}

static int do_send(struct batch_ctx *ctx)
{
    if (ctx->record_count==0) return 0;
    size_t cap=(size_t)ctx->record_count*MAX_JSON_REC+32;
    char *payload=flb_malloc(cap);
    if (!payload) { buf_free_all(ctx); return -1; }
    size_t pos=0;
    payload[pos++]='[';
    for (int i=0; i<ctx->record_count; i++) {
        if (!ctx->records[i]) continue;
        size_t jl=strlen(ctx->records[i]->json);
        if (i>0) payload[pos++]=',';
        if (pos+jl+2>=cap) break;
        memcpy(payload+pos,ctx->records[i]->json,jl); pos+=jl;
    }
    payload[pos++]=']'; payload[pos]='\0';

    int ret = do_send_http(ctx, payload, pos, ctx->record_count);
    flb_free(payload);
    buf_free_all(ctx);
    return ret;
}

static int send_single(struct batch_ctx *ctx, const char *json)
{
    size_t jlen = strlen(json);
    size_t plen = jlen + 2;
    char *payload = flb_malloc(plen + 1);
    if (!payload) return -1;
    payload[0] = '[';
    memcpy(payload + 1, json, jlen);
    payload[jlen + 1] = ']';
    payload[jlen + 2] = '\0';
    int ret = do_send_http(ctx, payload, plen, 1);
    flb_free(payload);
    return ret;
}

static int cb_batch_init(struct flb_output_instance *ins,
                          struct flb_config *config, void *data)
{
    struct batch_ctx *ctx=flb_calloc(1,sizeof(struct batch_ctx));
    if (!ctx) { flb_errno(); return -1; }
    ctx->ins=ins;
    pthread_mutex_init(&ctx->lock,NULL);
    safe_strcpy(ctx->host,"127.0.0.1",MAX_HOST_LEN);
    ctx->port=8080;
    safe_strcpy(ctx->path,"/",MAX_PATH_LEN);
    ctx->batch_size=200;
    ctx->batch_timeout_sec=2;
    ctx->collapse_alerts=0;
    ctx->retry_limit=3;
    ctx->retry_delay_sec=1;

    ctx->seen.cap=256;
    ctx->seen.keys=flb_calloc((size_t)ctx->seen.cap,sizeof(char *));
    if (!ctx->seen.keys) { flb_free(ctx); return -1; }

    const char *val;
    if ((val=flb_output_get_property("host",ins)))             safe_strcpy(ctx->host,val,MAX_HOST_LEN);
    if ((val=flb_output_get_property("port",ins)))             ctx->port=atoi(val);
    if ((val=flb_output_get_property("path",ins)))             safe_strcpy(ctx->path,val,MAX_PATH_LEN);
    if ((val=flb_output_get_property("batch_size",ins)))       ctx->batch_size=atoi(val);
    if ((val=flb_output_get_property("batch_timeout_sec",ins)))ctx->batch_timeout_sec=atoi(val);
    if ((val=flb_output_get_property("collapse_alerts",ins)))
        ctx->collapse_alerts=(strcasecmp(val,"true")==0||strcmp(val,"1")==0)?1:0;
    if ((val=flb_output_get_property("retry_limit",ins)))      ctx->retry_limit=atoi(val);
    if ((val=flb_output_get_property("retry_delay_sec",ins)))  ctx->retry_delay_sec=atoi(val);
    if (ctx->batch_size>MAX_BATCH_SIZE) ctx->batch_size=MAX_BATCH_SIZE;

    ctx->records=flb_calloc(ctx->batch_size,sizeof(struct buf_record *));
    if (!ctx->records) {
        flb_free(ctx->seen.keys);
        pthread_mutex_destroy(&ctx->lock); flb_free(ctx); return -1;
    }
    ctx->upstream=flb_upstream_create(config,ctx->host,ctx->port,FLB_IO_TCP,NULL);
    if (!ctx->upstream) {
        flb_free(ctx->records);
        flb_free(ctx->seen.keys);
        pthread_mutex_destroy(&ctx->lock); flb_free(ctx); return -1;
    }
    flb_output_upstream_set(ctx->upstream,ins);
    flb_output_set_context(ins,ctx);
    flb_plg_info(ins,"ready host=%s port=%d path=%s batch=%d timeout=%ds collapse=%s first_occ=immediate",
                 ctx->host,ctx->port,ctx->path,ctx->batch_size,ctx->batch_timeout_sec,
                 ctx->collapse_alerts?"true":"false");
    return 0;
}

static void cb_batch_flush(struct flb_event_chunk *event_chunk,
                            struct flb_output_flush *out_flush,
                            struct flb_input_instance *i_ins,
                            void *out_context,
                            struct flb_config *config)
{
    struct batch_ctx *ctx=out_context;
    msgpack_unpacked result;
    size_t off=0;
    msgpack_unpacked_init(&result);
    pthread_mutex_lock(&ctx->lock);

    while (msgpack_unpack_next(&result,
                               event_chunk->data,event_chunk->size,&off)
           ==MSGPACK_UNPACK_SUCCESS) {

        msgpack_object root=result.data;
        if (root.type!=MSGPACK_OBJECT_ARRAY||root.via.array.size!=2) continue;
        msgpack_object map=root.via.array.ptr[1];
        if (map.type!=MSGPACK_OBJECT_MAP) continue;

        char *level_buf  = flb_calloc(1, 64);
        char *msg_buf    = flb_calloc(1, MAX_FIELD_LEN);
        char *file_buf   = flb_calloc(1, MAX_FIELD_LEN);
        long  line_v     = 0;

        if (!level_buf || !msg_buf || !file_buf) {
            flb_free(level_buf); flb_free(msg_buf); flb_free(file_buf);
            continue;
        }

        for (uint32_t i=0; i<map.via.map.size; i++) {
            msgpack_object k=map.via.map.ptr[i].key;
            msgpack_object v=map.via.map.ptr[i].val;
            if (k.type!=MSGPACK_OBJECT_STR) continue;
#define MATCH(n) (k.via.str.size==strlen(n)&&strncmp(k.via.str.ptr,(n),k.via.str.size)==0)
            if (MATCH("level")&&v.type==MSGPACK_OBJECT_STR) {
                size_t l=v.via.str.size<63?v.via.str.size:63;
                memcpy(level_buf,v.via.str.ptr,l);
            }
            else if (MATCH("message")&&v.type==MSGPACK_OBJECT_STR) {
                size_t l=v.via.str.size<MAX_FIELD_LEN-1?v.via.str.size:MAX_FIELD_LEN-1;
                memcpy(msg_buf,v.via.str.ptr,l);
            }
            else if (MATCH("file")&&v.type==MSGPACK_OBJECT_STR) {
                size_t l=v.via.str.size<MAX_FIELD_LEN-1?v.via.str.size:MAX_FIELD_LEN-1;
                memcpy(file_buf,v.via.str.ptr,l);
            }
            else if (MATCH("line")) {
                if(v.type==MSGPACK_OBJECT_POSITIVE_INTEGER) line_v=(long)v.via.u64;
                else if(v.type==MSGPACK_OBJECT_NEGATIVE_INTEGER) line_v=(long)v.via.i64;
            }
#undef MATCH
        }

        char *alert_key = flb_calloc(1, MAX_KEY_LEN);
        if (ctx->collapse_alerts && alert_key) {
            char *k = make_alert_key(level_buf, msg_buf, file_buf, line_v);
            if (k) { safe_strcpy(alert_key, k, MAX_KEY_LEN); flb_free(k); }
        }

        flb_free(level_buf);
        flb_free(msg_buf);
        flb_free(file_buf);

        char *json = flb_malloc(MAX_JSON_REC);
        if (!json) { flb_free(alert_key); continue; }

        if (map_to_json(map, json, MAX_JSON_REC)!=0) {
            flb_free(json); flb_free(alert_key); continue;
        }

        /* First-occurrence check: send immediately if new key */
        if (ctx->collapse_alerts && alert_key && alert_key[0]!='\0') {
            if (!seen_contains(&ctx->seen, alert_key)) {
                seen_add(&ctx->seen, alert_key);
                pthread_mutex_unlock(&ctx->lock);
                flb_plg_info(ctx->ins, "FIRST OCCURRENCE sending immediately");
                send_single(ctx, json);
                pthread_mutex_lock(&ctx->lock);
                flb_free(json);
                flb_free(alert_key);
                continue;
            }
        }

        if (ctx->record_count==0) ctx->oldest_ts=time(NULL);
        buf_add(ctx, alert_key ? alert_key : "", json);
        flb_free(json);
        flb_free(alert_key);

        double age=difftime(time(NULL),ctx->oldest_ts);
        int do_flush=(ctx->record_count>=ctx->batch_size)||
                     (ctx->oldest_ts>0&&age>=(double)ctx->batch_timeout_sec);
        if (do_flush) do_send(ctx);
    }

    msgpack_unpacked_destroy(&result);
    pthread_mutex_unlock(&ctx->lock);
    FLB_OUTPUT_RETURN(FLB_OK);
}

static int cb_batch_exit(void *data, struct flb_config *config)
{
    struct batch_ctx *ctx=data;
    if (!ctx) return 0;
    pthread_mutex_lock(&ctx->lock);
    if (ctx->record_count>0) {
        flb_plg_info(ctx->ins,"shutdown flush: %d record(s)",ctx->record_count);
        do_send(ctx);
    }
    pthread_mutex_unlock(&ctx->lock);
    pthread_mutex_destroy(&ctx->lock);
    for (int i=0; i<ctx->seen.count; i++)
        flb_free(ctx->seen.keys[i]);
    flb_free(ctx->seen.keys);
    flb_free(ctx->records);
    flb_free(ctx);
    return 0;
}

struct flb_output_plugin out_batchhttp_plugin = {
    .name        = "batch_http",
    .description = "Batching + alert-collapsing HTTP output",
    .cb_init     = cb_batch_init,
    .cb_flush    = cb_batch_flush,
    .cb_exit     = cb_batch_exit,
    .flags       = 0
};
