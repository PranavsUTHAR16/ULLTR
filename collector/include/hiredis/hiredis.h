#pragma once

#include <cstddef>
#include <cstdint>

#define REDIS_REPLY_STRING 1
#define REDIS_REPLY_ARRAY 2
#define REDIS_REPLY_INTEGER 3
#define REDIS_REPLY_NIL 4
#define REDIS_REPLY_STATUS 5
#define REDIS_REPLY_ERROR 6

struct redisContext {
    int err;
    char errstr[128];
    int fd;
    int flags;
    char *obuf;
};

struct redisReply {
    int type;
    long long integer;
    double dval;
    size_t len;
    char *str;
    size_t elements;
    struct redisReply **element;
};

#ifdef __cplusplus
extern "C" {
#endif

void freeReplyObject(void *reply);
void *redisCommand(redisContext *c, const char *format, ...);
redisContext *redisConnect(const char *ip, int port);
redisContext *redisConnectUnix(const char *path);
void redisFree(redisContext *c);

#ifdef __cplusplus
}
#endif
