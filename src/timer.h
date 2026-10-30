#pragma once
#include <chrono>

class Timer {
    public:
        Timer(int interval);
        bool is_timer_ready();
    private:
        std::chrono::milliseconds interval;
        std::chrono::time_point<std::chrono::steady_clock> last_time;
};