#include "timer.h"

#include <chrono>

Timer::Timer(int interval) {
    this->interval = std::chrono::milliseconds(interval);
}

bool Timer::is_timer_ready() {
    auto now = std::chrono::steady_clock::now();
    if (now - last_time >= interval) {
        last_time = now;
        return true;
    }
    return false;
}