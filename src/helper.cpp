#include "helper.h"

std::string env(const char *key, const char *fallback) {
    const char *value = std::getenv(key);
    if (value == nullptr) {
        return fallback;
    }
    return value;
}