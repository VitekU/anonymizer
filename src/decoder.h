#include <iostream>

typedef struct {
    uint64_t timestampEpochMilli;
    uint64_t resourceId;
    uint64_t bytesSent;
    uint64_t requestTimeMilli;
    uint16_t responseStatus;
    std::string cacheStatus;
    std::string method;
    std::string remoteAddr;
    std::string url;
} HttpLog;

bool get_log(const void *buf, size_t len, HttpLog *log);