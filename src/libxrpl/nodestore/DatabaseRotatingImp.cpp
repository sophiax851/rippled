#include <xrpl/nodestore/detail/DatabaseRotatingImp.h>

#include <xrpl/basics/BasicConfig.h>
#include <xrpl/basics/Blob.h>
#include <xrpl/basics/Log.h>
#include <xrpl/basics/base_uint.h>
#include <xrpl/basics/contract.h>
#include <xrpl/beast/utility/Journal.h>
#include <xrpl/nodestore/Backend.h>
#include <xrpl/nodestore/Database.h>
#include <xrpl/nodestore/DatabaseRotating.h>
#include <xrpl/nodestore/NodeObject.h>
#include <xrpl/nodestore/Scheduler.h>
#include <xrpl/nodestore/Types.h>

#include <cstdint>
#include <cstdlib>
#include <exception>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace xrpl::NodeStore {

namespace {
// RAII counter guard for in-flight store/fetch tracking. Must remain
// noexcept and trivial so it does not affect the hot path beyond two
// relaxed atomic ops.
struct InFlightGuard
{
    std::atomic<std::int64_t>& counter;
    explicit InFlightGuard(std::atomic<std::int64_t>& c) noexcept : counter(c)
    {
        counter.fetch_add(1, std::memory_order_acq_rel);
    }
    ~InFlightGuard()
    {
        counter.fetch_sub(1, std::memory_order_acq_rel);
    }
    InFlightGuard(InFlightGuard const&) = delete;
    InFlightGuard&
    operator=(InFlightGuard const&) = delete;
};

// Debug instrumentation: when RIPPLED_TRACE_NODE_HASH is set to a node hash,
// the online-delete copy/freshen path emits targeted debug lines for that one
// node so a specific copy-forward durability gap can be traced. Unset (the
// default) makes every probe a single optional test on the hot path.
std::optional<uint256> const&
traceNodeHash()
{
    static std::optional<uint256> const h = []() -> std::optional<uint256> {
        if (char const* e = std::getenv("RIPPLED_TRACE_NODE_HASH"))
        {
            uint256 u;
            if (u.parseHex(e))
                return u;
        }
        return std::nullopt;
    }();
    return h;
}
}  // namespace

DatabaseRotatingImp::DatabaseRotatingImp(
    Scheduler& scheduler,
    int readThreads,
    std::shared_ptr<Backend> writableBackend,
    std::shared_ptr<Backend> archiveBackend,
    Section const& config,
    beast::Journal j)
    : DatabaseRotating(scheduler, readThreads, config, j)
    , writableBackend_(std::move(writableBackend))
    , archiveBackend_(std::move(archiveBackend))
{
    if (writableBackend_)
        fdRequired_ += writableBackend_->fdRequired();
    if (archiveBackend_)
        fdRequired_ += archiveBackend_->fdRequired();
}

void
DatabaseRotatingImp::rotate(
    std::unique_ptr<NodeStore::Backend>&& newBackend,
    std::function<void(std::string const& writableName, std::string const& archiveName)> const& f)
{
    // Pass these two names to the callback function
    std::string const newWritableBackendName = newBackend->getName();
    std::string newArchiveBackendName;
    std::string oldArchiveBackendName;
    // Hold on to current archive backend pointer until after the
    // callback finishes. Only then will the archive directory be
    // deleted.
    std::shared_ptr<NodeStore::Backend> oldArchiveBackend;
    std::uint64_t newGen = 0;
    std::int64_t pendingStores = 0;
    std::int64_t pendingFetches = 0;
    std::uint64_t prevStoreRaces = 0;
    std::uint64_t prevFetchRaces = 0;
    std::uint64_t prevFetchMisses = 0;
    {
        std::scoped_lock const lock(mutex_);

        oldArchiveBackendName = archiveBackend_->getName();
        archiveBackend_->setDeletePath();
        oldArchiveBackend = std::move(archiveBackend_);

        archiveBackend_ = std::move(writableBackend_);
        newArchiveBackendName = archiveBackend_->getName();

        writableBackend_ = std::move(newBackend);

        // Bump generation under the lock so store()/fetchNodeObject()
        // callers that captured the old writable pointer can detect the
        // swap via genBefore != genAfter.
        newGen = rotationGen_.fetch_add(1, std::memory_order_acq_rel) + 1;
        pendingStores = inFlightStores_.load(std::memory_order_acquire);
        pendingFetches = inFlightFetches_.load(std::memory_order_acquire);

        // Snapshot and reset the per-rotation event counters so the next
        // window starts clean and the SWAP log line carries the totals
        // from the window just ended.
        prevStoreRaces = storeRaceCount_.exchange(0, std::memory_order_acq_rel);
        prevFetchRaces = fetchRaceCount_.exchange(0, std::memory_order_acq_rel);
        prevFetchMisses = fetchMissCount_.exchange(0, std::memory_order_acq_rel);
    }

    if (pendingStores > 0 || pendingFetches > 0 || prevStoreRaces > 0 ||
        prevFetchRaces > 0 || prevFetchMisses > 0)
    {
        JLOG(j_.warn())
            << "Rotating: SWAP gen=" << newGen
            << " inFlightStores=" << pendingStores
            << " inFlightFetches=" << pendingFetches
            << " prevWindowStoreRaces=" << prevStoreRaces
            << " prevWindowFetchRaces=" << prevFetchRaces
            << " prevWindowFetchMisses=" << prevFetchMisses
            << " newWritable=" << newWritableBackendName
            << " demotedToArchive=" << newArchiveBackendName
            << " markedForDelete=" << oldArchiveBackendName;
    }
    else
    {
        JLOG(j_.debug())
            << "Rotating: SWAP clean gen=" << newGen
            << " newWritable=" << newWritableBackendName
            << " demotedToArchive=" << newArchiveBackendName
            << " markedForDelete=" << oldArchiveBackendName;
    }

    f(newWritableBackendName, newArchiveBackendName);
}

std::string
DatabaseRotatingImp::getName() const
{
    std::scoped_lock const lock(mutex_);
    return writableBackend_->getName();
}

std::int32_t
DatabaseRotatingImp::getWriteLoad() const
{
    std::scoped_lock const lock(mutex_);
    return writableBackend_->getWriteLoad();
}

void
DatabaseRotatingImp::importDatabase(Database& source)
{
    auto const backend = [&] {
        std::scoped_lock const lock(mutex_);
        return writableBackend_;
    }();

    importInternal(*backend, source);
}

void
DatabaseRotatingImp::sync()
{
    std::scoped_lock const lock(mutex_);
    writableBackend_->sync();
}

std::pair<std::string, std::string>
DatabaseRotatingImp::getBackendNames() const
{
    std::scoped_lock const lock(mutex_);
    return {writableBackend_->getName(), archiveBackend_->getName()};
}

void
DatabaseRotatingImp::store(NodeObjectType type, Blob&& data, uint256 const& hash, std::uint32_t)
{
    InFlightGuard const ifg(inFlightStores_);
    auto const genBefore = rotationGen_.load(std::memory_order_acquire);

    auto nObj = NodeObject::createObject(type, std::move(data), hash);

    auto const backend = [&] {
        std::scoped_lock const lock(mutex_);
        return writableBackend_;
    }();
    auto const backendName = backend->getName();

    backend->store(nObj);
    storeStats(1, nObj->getData().size());

    auto const genAfter = rotationGen_.load(std::memory_order_acquire);
    if (genAfter != genBefore)
    {
        auto const delta = genAfter - genBefore;
        auto const n = ++storeRaceCount_;
        // delta == 1 : the write landed in what is now the archive backend.
        //              The node is still on disk and a duplicate=true fetch
        //              will pull it back, but the next rotation will drop
        //              the archive unless copyNode/freshen re-stores it.
        // delta >= 2 : the write landed in a backend already marked for
        //              deletion. As soon as this store() releases its
        //              shared_ptr, the backend destructor will delete the
        //              files. This is silent permanent loss.
        // delta>=2 always logs (errors are rare and individually critical);
        // delta==1 is throttled to first kMaxLoggedPerRotation per window.
        if (delta >= 2 || n <= kMaxLoggedPerRotation)
        {
            auto const& strm = (delta >= 2) ? j_.error() : j_.warn();
            JLOG(strm)
                << "Rotating: store RACED rotation hash=" << hash
                << " backend=" << backendName
                << " genBefore=" << genBefore << " genAfter=" << genAfter
                << " delta=" << delta
                << (delta == 1 ? " (wrote into demoted-to-archive backend)"
                               : " (wrote into backend slated for DELETION)")
                << (delta == 1 && n == kMaxLoggedPerRotation
                        ? " (further per-store RACED warns suppressed; see "
                          "next SWAP for window total)"
                        : "");
        }
    }
}

std::shared_ptr<NodeObject>
DatabaseRotatingImp::fetchNodeObject(
    uint256 const& hash,
    std::uint32_t,
    FetchReport& fetchReport,
    bool duplicate)
{
    InFlightGuard const ifg(inFlightFetches_);
    auto const genBefore = rotationGen_.load(std::memory_order_acquire);
    auto const& traceTarget = traceNodeHash();

    auto fetch = [&](std::shared_ptr<Backend> const& backend) {
        Status status = Status::Ok;
        std::shared_ptr<NodeObject> nodeObject;
        try
        {
            status = backend->fetch(hash, &nodeObject);
        }
        catch (std::exception const& e)
        {
            JLOG(j_.fatal()) << "Exception, " << e.what();
            rethrow();
        }

        switch (status)
        {
            case Status::Ok:
            case Status::NotFound:
                break;
            case Status::DataCorrupt:
                JLOG(j_.fatal()) << "Corrupt NodeObject #" << hash;
                break;
            default:
                JLOG(j_.warn()) << "Unknown status=" << static_cast<int>(status);
                break;
        }

        return nodeObject;
    };

    // See if the node object exists in the cache
    std::shared_ptr<NodeObject> nodeObject;

    auto [writable, archive] = [&] {
        std::scoped_lock const lock(mutex_);
        return std::make_pair(writableBackend_, archiveBackend_);
    }();

    // Try to fetch from the writable backend
    nodeObject = fetch(writable);
    bool foundInArchive = false;
    if (!nodeObject)
    {
        // Otherwise try to fetch from the archive backend
        nodeObject = fetch(archive);
        if (nodeObject)
        {
            foundInArchive = true;
            {
                // Refresh the writable backend pointer
                std::scoped_lock const lock(mutex_);
                writable = writableBackend_;
            }

            // Update writable backend with data from the archive backend
            if (duplicate)
            {
                writable->store(nodeObject);

                // Always-on copy-forward durability check: a node just
                // restored from the archive must be immediately readable from
                // the writable it was stored into. This runs only on the
                // archive-hit path (the only place a copy-forward store
                // happens) and logs only on failure, so steady-state volume is
                // zero. It catches the silent copy-forward gap for ANY node,
                // with no need to know the hash in advance.
                std::shared_ptr<NodeObject> readBack;
                auto const st = writable->fetch(hash, &readBack);
                if (!readBack)
                {
                    JLOG(j_.error())
                        << "Rotating: verify-after-store FAILED hash=" << hash
                        << " writable=" << writable->getName()
                        << " status=" << static_cast<int>(st)
                        << " (copy-forward store not immediately readable)";
                }
                else if (traceTarget && hash == *traceTarget)
                {
                    JLOG(j_.debug())
                        << "Rotating: TRACE verify-after-store hash=" << hash
                        << " writable=" << writable->getName()
                        << " readBack=ok status=" << static_cast<int>(st);
                }
            }
        }
        else if (duplicate)
        {
            // duplicate=true means the caller (online-delete copy/freshen)
            // expected to be able to refresh this node into writable. Missing
            // from both backends is the silent-loss signal we want to catch.
            auto const n = ++fetchMissCount_;
            if (n <= kMaxLoggedPerRotation)
            {
                JLOG(j_.warn())
                    << "Rotating: fetchNodeObject MISS (both backends) hash="
                    << hash << " writable=" << writable->getName()
                    << " archive=" << archive->getName()
                    << (n == kMaxLoggedPerRotation
                            ? " (further per-fetch MISS warns suppressed; "
                              "see next SWAP for window total)"
                            : "");
            }
        }
    }

    // Target-gated location probe: record which backend served the target
    // node (or none) for copy/freshen (duplicate=true) and normal reads.
    if (traceTarget && hash == *traceTarget)
    {
        JLOG(j_.debug())
            << "Rotating: TRACE fetchNodeObject hash=" << hash
            << " duplicate=" << (duplicate ? "yes" : "no") << " foundIn="
            << (nodeObject ? (foundInArchive ? "archive" : "writable") : "none")
            << " writable=" << writable->getName()
            << " archive=" << archive->getName();
    }

    if (nodeObject)
        fetchReport.wasFound = true;

    auto const genAfter = rotationGen_.load(std::memory_order_acquire);
    if (genAfter != genBefore)
    {
        // The backend pointers used for this fetch are now stale. Reads
        // are physically safe (the shared_ptr kept the backend alive),
        // but a MISS here could be a false negative: the node may have
        // landed in the new writable after we captured the old one.
        auto const n = ++fetchRaceCount_;
        if (n <= kMaxLoggedPerRotation)
        {
            JLOG(j_.warn())
                << "Rotating: fetchNodeObject RACED rotation hash=" << hash
                << " genBefore=" << genBefore << " genAfter=" << genAfter
                << " delta=" << (genAfter - genBefore)
                << " found=" << (nodeObject ? "yes" : "no")
                << " duplicate=" << (duplicate ? "yes" : "no")
                << (n == kMaxLoggedPerRotation
                        ? " (further per-fetch RACED warns suppressed; "
                          "see next SWAP for window total)"
                        : "");
        }
    }

    return nodeObject;
}

void
DatabaseRotatingImp::forEach(std::function<void(std::shared_ptr<NodeObject>)> f)
{
    auto [writable, archive] = [&] {
        std::scoped_lock const lock(mutex_);
        return std::make_pair(writableBackend_, archiveBackend_);
    }();

    // Iterate the writable backend
    writable->forEach(f);

    // Iterate the archive backend
    archive->forEach(f);
}

}  // namespace xrpl::NodeStore
