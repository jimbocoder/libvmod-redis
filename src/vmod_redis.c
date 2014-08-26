#include <stdlib.h>
#include <stdio.h>
#include <sys/time.h>

#include <pthread.h>
#include <hiredis/hiredis.h>

#include "vrt.h"
#include "cache/cache.h"
#include "vcc_if.h"

#define REDIS_TIMEOUT_MS    200
#define REDIS_DEFAULT_PORT  6379
#define LOG_E(...) fprintf(stderr, __VA_ARGS__);

typedef void (*thread_destructor)(void *);

typedef struct redisConfig {
    char *host;
    int port;
    struct timeval timeout;
} config_t;

static pthread_key_t thread_key;
static pthread_once_t thread_once = PTHREAD_ONCE_INIT;

/**
 * Free our private config structure.
 */
static void free_config(config_t *cfg)
{
  free(cfg->host);
  free(cfg);
}

/**
 * Create a thread specific data key visible to all threads in the process and
 * register redisFree as the destructor to cleanup the redis connection.
 */
static void make_key()
{
    pthread_key_create(&thread_key, (thread_destructor)redisFree);
}

/**
 * Build a private config structure with the given arguments.
 */
static config_t *make_config(const char *host, int port, int timeout_ms)
{
    config_t *cfg;

    cfg = malloc(sizeof(config_t));

    if (cfg == NULL) {
        return NULL;
    }

    if (port <= 0) {
        port = REDIS_DEFAULT_PORT;
    }

    if (timeout_ms <= 0) {
        timeout_ms = REDIS_TIMEOUT_MS;
    }

    cfg->host = strdup(host);
    cfg->port = port;
    cfg->timeout.tv_sec = timeout_ms / 1000;
    cfg->timeout.tv_usec = (timeout_ms % 1000) * 1000;

    return cfg;
}

/**
 * This init function is called whenever a VCL program imports this VMOD. Here
 * we create a new config with the default parameters (localhost, 6379, 200ms
 * timeout).
 */
int init_function(struct vmod_priv *priv, const struct VCL_conf *conf)
{
    config_t *cfg;

    pthread_once(&thread_once, make_key);

    if (priv->priv == NULL) {
        priv->priv = make_config("127.0.0.1", REDIS_DEFAULT_PORT, REDIS_TIMEOUT_MS);
        priv->free = (vmod_priv_free_f *)free_config;
    }

    return (0);
}

VCL_VOID
vmod_init_redis(const struct vrt_ctx* ctx, struct vmod_priv *priv, VCL_STRING host, VCL_INT port, VCL_INT timeout_ms)
{
    config_t *old_cfg = priv->priv;

    priv->priv = make_config(host, port, timeout_ms);

    if (old_cfg) {
        free_config(old_cfg);
    }
}

static redisContext *redis_connect(config_t *cfg)
{
    redisContext *c;

    if ((c = pthread_getspecific(thread_key)) == NULL) {
        c = redisConnectWithTimeout(cfg->host, cfg->port, cfg->timeout);

        if (c->err) {
            LOG_E("redis error (connect): %s\n", c->errstr);
            redisFree(c);
            return NULL;
        }

        pthread_setspecific(thread_key, c);
    }

    return c;
}

static redisReply *redis_common(const struct vrt_ctx* ctx, struct vmod_priv *priv, const char *command)
{
    config_t *cfg = priv->priv;
    redisContext *c = redis_connect(cfg);

    if (c != NULL) {
        redisReply *reply = redisCommand(c, command);

        if (reply == NULL) {
            LOG_E("redis error (command): err=%d errstr=%s\n", c->err, c->errstr);
        } else {
            return reply;
        }
    }

    return NULL;
}

VCL_VOID
vmod_send(const struct vrt_ctx* ctx, struct vmod_priv *priv, VCL_STRING command)
{
    redisReply *reply = redis_common(ctx, priv, command);

    if (reply != NULL) {
        freeReplyObject(reply);
    }
}

VCL_STRING
vmod_call(const struct vrt_ctx* ctx, struct vmod_priv *priv, const char *command)
{
    redisReply *reply = NULL;
    const char *ret = NULL;
    char *digits;

    reply = redis_common(ctx, priv, command);

    if (reply == NULL) {
        goto done;
    }

    switch (reply->type) {
        case REDIS_REPLY_STATUS:
            ret = WS_Printf(ctx->ws, "%s", reply->str);
            break;
        case REDIS_REPLY_ERROR:
            ret = WS_Printf(ctx->ws, "%s", reply->str);
            break;
        case REDIS_REPLY_INTEGER:
            digits = WS_Alloc(ctx->ws, 21); /* sizeof(long long) == 8; 20 digits + NUL */
            if (digits) {
                sprintf(digits, "%lld", reply->integer);
            }
            ret = digits;
            break;
        case REDIS_REPLY_NIL:
            ret = NULL;
            break;
        case REDIS_REPLY_STRING:
            ret = WS_Printf(ctx->ws, "%s", reply->str);
            break;
        case REDIS_REPLY_ARRAY:
            ret = WS_Printf(ctx->ws, "%s", "array");
            break;
        default:
            ret = WS_Printf(ctx->ws, "%s", "unexpected");
    }

done:
    if (reply) {
        freeReplyObject(reply);
    }

    return ret;
}

VCL_VOID
vmod_pipeline(const struct vrt_ctx* ctx, struct vmod_priv *priv)
{
    redis_connect(priv->priv);
}

VCL_VOID
vmod_push(const struct vrt_ctx* ctx, struct vmod_priv *priv, const char *command)
{
    redisContext *c = pthread_getspecific(thread_key);

    if (c != NULL) {
        redisAppendCommand(c, command);
    }
}

VCL_STRING
vmod_pop(const struct vrt_ctx* ctx, struct vmod_priv *priv)
{
    redisReply *reply;
    const char *ret = NULL;
    char *digits;
    redisContext *c;

    c = pthread_getspecific(thread_key);

    if (c == NULL) {
        goto done;
    } else {
        redisGetReply(c, &reply);
    }

    if (reply == NULL) {
        LOG_E("redis error (command): err=%d errstr=%s\n", c->err, c->errstr);
        goto done;
    }

    switch (reply->type) {
        case REDIS_REPLY_STATUS:
            ret = WS_Printf(ctx->ws, "%s", reply->str);
            break;
        case REDIS_REPLY_ERROR:
            ret = WS_Printf(ctx->ws, "%s", reply->str);
            break;
        case REDIS_REPLY_INTEGER:
            digits = WS_Alloc(ctx->ws, 21); /* sizeof(long long) == 8; 20 digits + NUL */
            if(digits)
                sprintf(digits, "%lld", reply->integer);
            ret = digits;
            break;
        case REDIS_REPLY_NIL:
            ret = NULL;
            break;
        case REDIS_REPLY_STRING:
            ret = WS_Printf(ctx->ws, "%s", reply->str);
            break;
        case REDIS_REPLY_ARRAY:
            ret = WS_Printf(ctx->ws, "%s", "array");
            break;
        default:
            ret = WS_Printf(ctx->ws, "%s", "unexpected");
    }

done:
    if (reply) {
        freeReplyObject(reply);
    }

    return ret;
}
