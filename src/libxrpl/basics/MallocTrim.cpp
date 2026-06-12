#include <xrpl/basics/MallocTrim.h>

#include <xrpl/basics/Log.h>
#include <xrpl/beast/utility/Journal.h>

#include <boost/predef.h>

#include <string_view>

#if defined(__GLIBC__) && BOOST_OS_LINUX
#include <sys/resource.h>

#include <malloc.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <ios>
#include <sstream>
#include <string>

// Require RUSAGE_THREAD for thread-scoped page fault tracking
#ifndef RUSAGE_THREAD
#error "MallocTrim rusage instrumentation requires RUSAGE_THREAD on Linux/glibc"
#endif

namespace {

bool
getRusageThread(struct rusage& ru)
{
    return ::getrusage(RUSAGE_THREAD, &ru) == 0;  // LCOV_EXCL_LINE
}

}  // namespace
#endif

namespace xrpl {

namespace detail {

// cSpell:ignore statm

#if defined(__GLIBC__) && BOOST_OS_LINUX

inline int
mallocTrimWithPad(std::size_t padBytes)
{
    return ::malloc_trim(padBytes);
}

long
parseStatmRSSkB(std::string const& statm)
{
    // /proc/self/statm format: size resident shared text lib data dt
    // We want the second field (resident) which is in pages
    std::istringstream iss(statm);
    long size = 0, resident = 0;
    if (!(iss >> size >> resident))
        return -1;

    // Convert pages to KB
    long const pageSize = ::sysconf(_SC_PAGESIZE);
    if (pageSize <= 0)
        return -1;

    return (resident * pageSize) / 1024;
}

#endif  // __GLIBC__ && BOOST_OS_LINUX

}  // namespace detail

MallocTrimReport
mallocTrim(std::string_view tag, beast::Journal journal)
{
    // LCOV_EXCL_START

    MallocTrimReport report;

#if !(defined(__GLIBC__) && BOOST_OS_LINUX)
    JLOG(journal.debug()) << "malloc_trim not supported on this platform (tag=" << tag << ")";
#else
    // Keep glibc malloc_trim padding at 0 (default): 12h Mainnet tests across 0/256KB/1MB/16MB
    // showed no clear, consistent benefit from custom padding—0 provided the best overall balance
    // of RSS reduction and trim-latency stability without adding a tuning surface.
    static constexpr std::size_t kTrimPad = 0;

    report.supported = true;

    // Threshold above which the trim is treated as a stall event: malloc_trim
    // holds the glibc arena lock for its entire duration, blocking malloc/free
    // on every other thread in the process. Emit at warn level so these events
    // are visible alongside other diagnostics without requiring debug logging.
    static constexpr std::chrono::microseconds kSlowThresholdUs{50'000};

    auto readFile = [](std::string const& path) -> std::string {
        std::ifstream ifs(path, std::ios::in | std::ios::binary);
        if (!ifs.is_open())
            return {};

        // /proc files are often not seekable; read as a stream.
        std::ostringstream oss;
        oss << ifs.rdbuf();
        return oss.str();
    };

    std::string const tagStr{tag};
    std::string const statmPath = "/proc/self/statm";

    auto const statmBefore = readFile(statmPath);
    long const rssBeforeKB = detail::parseStatmRSSkB(statmBefore);

    struct rusage ru0{};
    bool const haveRu0 = getRusageThread(ru0);

    // Process-wide rusage at entry. Used together with the previous call's
    // exit snapshot to compute the workload's RSS growth and minor-fault
    // count between trims, revealing whether trim is doing useful net work
    // or thrashing the same memory in and out on every cycle.
    struct rusage ruProcEntry{};
    bool const haveProcEntry = (::getrusage(RUSAGE_SELF, &ruProcEntry) == 0);

    static std::atomic<long> lastExitRssKB{-1};
    static std::atomic<long> lastExitProcMinflt{-1};
    static std::atomic<long long> lastExitSteadyNs{-1};

    long const prevExitRssKB = lastExitRssKB.load(std::memory_order_relaxed);
    long const prevExitProcMinflt = lastExitProcMinflt.load(std::memory_order_relaxed);
    long long const prevExitSteadyNs = lastExitSteadyNs.load(std::memory_order_relaxed);

    auto const t0 = std::chrono::steady_clock::now();
    long long const t0Ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t0.time_since_epoch()).count();

    report.trimResult = detail::mallocTrimWithPad(kTrimPad);

    auto const t1 = std::chrono::steady_clock::now();
    long long const t1Ns =
        std::chrono::duration_cast<std::chrono::nanoseconds>(t1.time_since_epoch()).count();

    struct rusage ru1{};
    bool const haveRu1 = getRusageThread(ru1);

    struct rusage ruProcExit{};
    bool const haveProcExit = (::getrusage(RUSAGE_SELF, &ruProcExit) == 0);

    auto const statmAfter = readFile(statmPath);
    long const rssAfterKB = detail::parseStatmRSSkB(statmAfter);

    // Refresh exit snapshot for the next call's between-trim comparison.
    if (rssAfterKB >= 0)
        lastExitRssKB.store(rssAfterKB, std::memory_order_relaxed);
    if (haveProcExit)
        lastExitProcMinflt.store(
            static_cast<long>(ruProcExit.ru_minflt), std::memory_order_relaxed);
    lastExitSteadyNs.store(t1Ns, std::memory_order_relaxed);

    report.rssBeforeKB = rssBeforeKB;
    report.rssAfterKB = rssAfterKB;
    report.durationUs = std::chrono::duration_cast<std::chrono::microseconds>(t1 - t0);

    if (haveRu0 && haveRu1)
    {
        report.minfltDelta = ru1.ru_minflt - ru0.ru_minflt;
        report.majfltDelta = ru1.ru_majflt - ru0.ru_majflt;
    }

    std::int64_t const deltaKB = (rssBeforeKB < 0 || rssAfterKB < 0)
        ? 0
        : (static_cast<std::int64_t>(rssAfterKB) - static_cast<std::int64_t>(rssBeforeKB));

    bool const haveBetween = prevExitRssKB >= 0 && prevExitProcMinflt >= 0
        && prevExitSteadyNs >= 0 && haveProcEntry && rssBeforeKB >= 0;
    std::string betweenSuffix;
    if (haveBetween)
    {
        std::int64_t const betweenMs = (t0Ns - prevExitSteadyNs) / 1'000'000;
        std::int64_t const rssGrowthKB =
            static_cast<std::int64_t>(rssBeforeKB) - static_cast<std::int64_t>(prevExitRssKB);
        std::int64_t const procMinfltDelta =
            static_cast<std::int64_t>(ruProcEntry.ru_minflt)
            - static_cast<std::int64_t>(prevExitProcMinflt);
        std::ostringstream b;
        b << " between_trims_ms=" << betweenMs
          << " rss_growth_kB=" << rssGrowthKB
          << " process_minflt=" << procMinfltDelta;
        betweenSuffix = b.str();
    }

    bool const slow = report.durationUs > kSlowThresholdUs;
    auto stream = slow ? journal.warn() : journal.debug();
    JLOG(stream) << "malloc_trim tag=" << tagStr << " result=" << report.trimResult
                 << " pad=" << kTrimPad << " bytes"
                 << " rss_before=" << rssBeforeKB << "kB"
                 << " rss_after=" << rssAfterKB << "kB"
                 << " delta=" << deltaKB << "kB"
                 << " duration_us=" << report.durationUs.count()
                 << " minflt_delta=" << report.minfltDelta
                 << " majflt_delta=" << report.majfltDelta
                 << betweenSuffix
                 << (slow
                         ? " SLOW (arena lock held; allocations on each arena "
                           "may have blocked sequentially)"
                         : "");

#endif

    return report;

    // LCOV_EXCL_STOP
}

}  // namespace xrpl
