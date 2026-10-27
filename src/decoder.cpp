#include <capnp/serialize.h>
#include <cstdio>
#include <cstring>
#include <vector>
#include "http_log.capnp.h"
#include "decoder.h"

bool get_log(const void *buf, size_t len, HttpLog *log) {
    size_t nwords = (len + sizeof(capnp::word) - 1) / sizeof(capnp::word);
    std::vector<capnp::word> aligned(nwords);
    memcpy(aligned.data(), buf, len);

    try {
        capnp::FlatArrayMessageReader msg(kj::ArrayPtr<const capnp::word>(aligned.data(), aligned.size()));
        HttpLogRecord::Reader reader = msg.getRoot<HttpLogRecord>();


        log->timestampEpochMilli = reader.getTimestampEpochMilli();
        log->resourceId = reader.getResourceId();
        log->bytesSent = reader.getBytesSent();
        log->requestTimeMilli = reader.getRequestTimeMilli();
        log->responseStatus = reader.getResponseStatus();
        log->cacheStatus = reader.getCacheStatus();
        log->method = reader.getMethod();
        log->remoteAddr = reader.getRemoteAddr();
        log->url = reader.getUrl();
        return true;

    } catch (kj::Exception &e) {
        fprintf(stderr, "decode error: %s\n", e.getDescription().cStr());
        return false;
    }
}