#pragma once

#include <xrpl/beast/utility/Journal.h>

#include <atomic>
#include <chrono>
#include <thread>

namespace xrpl {

/**
 * @brief Independent watchdog thread that detects scheduling stalls.
 *
 * Sleeps for a fixed tick interval via std::this_thread::sleep_until and emits
 * a warn-level log whenever the actual wake-up is later than the expected
 * wake-up by more than a configured threshold. Captures application-level
 * stalls (mutex contention, malloc_trim arena holds, long getKeys() walks),
 * OS-level scheduling latency, kernel preemption, paging, and cgroup throttle
 * events without requiring instrumentation on the blocked path itself.
 *
 * Designed for diagnostic builds. One dedicated thread, one timed wait per
 * tick when healthy, no measurable cost beyond a single sleep.
 */
class StallWatcher
{
public:
    StallWatcher(
        beast::Journal journal,
        std::chrono::milliseconds tickInterval = std::chrono::milliseconds{100},
        std::chrono::milliseconds warnThreshold = std::chrono::milliseconds{5});

    ~StallWatcher();

    StallWatcher(StallWatcher const&) = delete;
    StallWatcher&
    operator=(StallWatcher const&) = delete;

    void
    start();

    void
    stop();

private:
    void
    run();

    beast::Journal journal_;
    std::chrono::milliseconds const tickInterval_;
    std::chrono::milliseconds const warnThreshold_;
    std::atomic<bool> stop_{false};
    std::thread thread_;
};

}  // namespace xrpl
