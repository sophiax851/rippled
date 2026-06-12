#include <xrpl/basics/StallWatcher.h>

#include <xrpl/basics/Log.h>
#include <xrpl/beast/core/CurrentThreadName.h>

#include <chrono>

namespace xrpl {

StallWatcher::StallWatcher(
    beast::Journal journal,
    std::chrono::milliseconds tickInterval,
    std::chrono::milliseconds warnThreshold)
    : journal_(journal), tickInterval_(tickInterval), warnThreshold_(warnThreshold)
{
}

StallWatcher::~StallWatcher()
{
    stop();
}

void
StallWatcher::start()
{
    if (thread_.joinable())
        return;
    stop_.store(false, std::memory_order_relaxed);
    thread_ = std::thread(&StallWatcher::run, this);
}

void
StallWatcher::stop()
{
    stop_.store(true, std::memory_order_relaxed);
    if (thread_.joinable())
        thread_.join();
}

void
StallWatcher::run()
{
    beast::setCurrentThreadName("StallWatch");
    JLOG(journal_.warn())
        << "StallWatch: started tickIntervalMs=" << tickInterval_.count()
        << " warnThresholdMs=" << warnThreshold_.count();

    auto nextWake = std::chrono::steady_clock::now() + tickInterval_;
    while (!stop_.load(std::memory_order_relaxed))
    {
        std::this_thread::sleep_until(nextWake);
        auto const actual = std::chrono::steady_clock::now();
        auto const lateness = std::chrono::duration_cast<std::chrono::milliseconds>(
            actual - nextWake);
        if (lateness > warnThreshold_)
        {
            JLOG(journal_.warn())
                << "StallWatch: lateMs=" << lateness.count()
                << " expectedTickMs=" << tickInterval_.count()
                << " thresholdMs=" << warnThreshold_.count();
        }
        nextWake = actual + tickInterval_;
    }

    JLOG(journal_.warn()) << "StallWatch: stopped";
}

}  // namespace xrpl
