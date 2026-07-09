#pragma once

#include <xrpld/app/ledger/LedgerMaster.h>
#include <xrpld/app/misc/SHAMapStore.h>

#include <xrpl/nodestore/DatabaseRotating.h>
#include <xrpl/nodestore/Scheduler.h>
#include <xrpl/rdb/DatabaseCon.h>
#include <xrpl/server/State.h>
#include <xrpl/shamap/SHAMapTreeNode.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <type_traits>

namespace xrpl {

class NetworkOPs;
class SHAMapNodeID;

class SHAMapStoreImp : public SHAMapStore
{
private:
    class SavedStateDB
    {
    public:
        soci::session sqlDb;
        std::mutex mutex;
        beast::Journal const journal;

        // Just instantiate without any logic in case online delete is not
        // configured
        explicit SavedStateDB() : journal{beast::Journal::getNullSink()}
        {
        }

        // opens database and, if necessary, creates & initializes its tables.
        void
        init(BasicConfig const& config, std::string const& dbName);
        // get/set the ledger index that we can delete up to and including
        LedgerIndex
        getCanDelete();
        LedgerIndex
        setCanDelete(LedgerIndex canDelete);
        SavedState
        getState();
        void
        setState(SavedState const& state);
        void
        setLastRotated(LedgerIndex seq);
    };

    Application& app_;

    // name of state database
    std::string const dbName_ = "state";
    // prefix of on-disk nodestore backend instances
    std::string const dbPrefix_ = "rippledb";  // cspell: disable-line
    // check health/stop status as records are copied
    std::uint64_t const checkHealthInterval_ = 1000;
    // emit a progress log line every N records during copy / freshen
    std::uint64_t const progressLogInterval_ = 100000;
    // cap on per-rotation detailed MISS log lines; aggregate counts are
    // always reported via the COPY_DONE / FRESHEN_DONE / SWAPPED markers
    static constexpr std::uint64_t kMaxLoggedPerRotation = 10;
    // minimum # of ledgers to maintain for health of network
    static std::uint32_t const kMinimumDeletionInterval = 256;
    // minimum # of ledgers required for standalone mode.
    static std::uint32_t const kMinimumDeletionIntervalSa = 8;
    // minimum ledger to maintain online.
    std::atomic<LedgerIndex> minimumOnline_;

    NodeStore::Scheduler& scheduler_;
    beast::Journal const journal_;
    NodeStore::DatabaseRotating* dbRotating_ = nullptr;
    SavedStateDB stateDb_;
    std::thread thread_;
    bool stop_ = false;
    bool healthy_ = true;
    mutable std::condition_variable cond_;
    mutable std::condition_variable rendezvous_;
    mutable std::mutex mutex_;
    std::shared_ptr<Ledger const> newLedger_;
    std::atomic<bool> working_;
    std::atomic<LedgerIndex> canDelete_;
    int fdRequired_ = 0;

    std::uint32_t deleteInterval_ = 0;
    bool advisoryDelete_ = false;
    std::uint32_t deleteBatch_ = 100;
    std::chrono::milliseconds backOff_{100};
    std::chrono::seconds ageThreshold_{60};
    /// If  the node is out of sync during an online_delete healthWait()
    /// call, sleep the thread for this time, and continue checking until
    /// recovery.
    /// See also: "recovery_wait_seconds" in xrpld-example.cfg
    std::chrono::seconds recoveryWaitTime_{5};

    // these do not exist upon SHAMapStore creation, but do exist
    // as of run() or before
    NetworkOPs* netOPs_ = nullptr;
    LedgerMaster* ledgerMaster_ = nullptr;

    // Correlation state for online-delete diagnostics. Updated by run()
    // at the start of each rotation pass and read by copyNode /
    // freshenCache so missing-node logs carry the same identifiers.
    std::atomic<std::uint64_t> rotationId_{0};
    std::atomic<std::uint32_t> copyingSeq_{0};
    std::atomic<std::uint64_t> copyMissCount_{0};
    std::atomic<std::uint64_t> freshenMissCount_{0};
    // Probe: of the freshen misses, how many still had their body resident
    // in the cache — i.e. how many a persist-on-miss repair could have
    // saved. Feeds the go/no-go decision on extending the copyNode re-store
    // into freshenCache.
    std::atomic<std::uint64_t> freshenMissBodyInCacheCount_{0};
    std::string writableName_;
    std::string archiveName_;

    static constexpr auto kNodeStoreName = "NodeStore";

public:
    SHAMapStoreImp(Application& app, NodeStore::Scheduler& scheduler, beast::Journal journal);

    std::uint32_t
    clampFetchDepth(std::uint32_t fetchDepth) const override
    {
        return (deleteInterval_ != 0u) ? std::min(fetchDepth, deleteInterval_) : fetchDepth;
    }

    std::unique_ptr<NodeStore::Database>
    makeNodeStore(int readThreads) override;

    LedgerIndex
    setCanDelete(LedgerIndex seq) override
    {
        if (advisoryDelete_)
            canDelete_ = seq;
        return stateDb_.setCanDelete(seq);
    }

    bool
    advisoryDelete() const override
    {
        return advisoryDelete_;
    }

    // All ledgers prior to this one are eligible
    // for deletion in the next rotation
    LedgerIndex
    getLastRotated() override
    {
        return stateDb_.getState().lastRotated;
    }

    // All ledgers before and including this are unprotected
    // and online delete may delete them if appropriate
    LedgerIndex
    getCanDelete() override
    {
        return canDelete_;
    }

    void
    onLedgerClosed(std::shared_ptr<Ledger const> const& ledger) override;

    void
    rendezvous() const override;
    int
    fdRequired() const override;

    std::optional<LedgerIndex>
    minimumOnline() const override;

private:
    // callback for visitNodes
    bool
    copyNode(std::uint64_t& nodeCount, SHAMapTreeNode const& node, SHAMapNodeID const& nodeID);
    void
    run();
    void
    dbPaths();

    // Debug/observability only: snapshot of the richer sync-state signals at the
    // rotation site. Used to check whether the "state full" label matches the
    // node's actual ledger completeness (needNetworkLedger / caughtUp /
    // complete-ledger range) when a missing node aborts the copy.
    std::string
    syncStateString();

    std::unique_ptr<NodeStore::Backend>
    makeBackendRotating(std::string path = std::string());

    // Warn threshold for getKeys(): the call holds the cache mutex, so a slow
    // walk here blocks every concurrent SHAMap lookup against the same cache.
    static constexpr std::chrono::milliseconds kGetKeysSlowThresholdMs{500};

    // logMisses marks a cache whose nodes live in the nodestore (TreeNodeCache):
    // misses are durability-relevant, so they are logged, counted in
    // freshenMisses, and the fetch requests copy-forward (duplicate=true). The
    // MasterTransactionCache passes logMisses=false: its tx nodes are absent
    // from the nodestore by design, so it must not log, count, request
    // copy-forward, nor inflate the backend fetchMissCount_.
    template <class CacheInstance>
    bool
    freshenCache(CacheInstance& cache, char const* cacheName, bool logMisses = true)
    {
        auto const getKeysT0 = std::chrono::steady_clock::now();
        auto const keys = cache.getKeys();
        auto const getKeysMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - getKeysT0);
        JLOG(journal_.warn()) << "SHAMapStore: freshen BEGIN rotation=" << rotationId_.load()
                              << " cache=" << cacheName << " cacheSize=" << keys.size()
                              << " getKeysMs=" << getKeysMs.count();
        if (getKeysMs > kGetKeysSlowThresholdMs)
        {
            JLOG(journal_.warn()) << "SHAMapStore: freshen GETKEYS_SLOW rotation="
                                  << rotationId_.load() << " cache=" << cacheName
                                  << " getKeysMs=" << getKeysMs.count()
                                  << " cacheSize=" << keys.size()
                                  << " (cache mutex held this long; lookups on other threads "
                                     "may have blocked)";
        }

        auto const loopT0 = std::chrono::steady_clock::now();
        std::uint64_t check = 0;
        std::uint64_t misses = 0;
        std::uint64_t missesBodyInCache = 0;
        for (auto const& key : keys)
        {
            // duplicate (copy-forward + backend miss-count) only for caches
            // whose nodes belong in the nodestore; see logMisses note above.
            auto const obj =
                dbRotating_->fetchNodeObject(key, 0, NodeStore::FetchType::Synchronous, logMisses);
            if (!obj)
            {
                ++misses;
                // Probe (log-only, nothing stored): would a persist-on-miss
                // repair be possible here? Only if the node body is still
                // resident in the cache. TreeNodeCache only: the tx cache
                // holds Transaction objects, not serializable tree nodes.
                // Note cache.fetch() touches the entry (refreshes its access
                // time); acceptable for the probe — it can only delay
                // eviction of a node that has no disk copy anyway.
                bool bodyInCache = false;
                int nodeType = -1;
                int leaf = -1;
                std::uint32_t nodeCowid = 0;
                if constexpr (std::is_same_v<typename CacheInstance::mapped_type, SHAMapTreeNode>)
                {
                    if (logMisses)
                    {
                        if (auto const node = cache.fetch(key))
                        {
                            bodyInCache = true;
                            ++missesBodyInCache;
                            nodeType = static_cast<int>(node->getType());
                            leaf = node->isLeaf() ? 1 : 0;
                            nodeCowid = node->cowid();
                        }
                    }
                }
                if (logMisses && misses <= kMaxLoggedPerRotation)
                {
                    JLOG(journal_.warn())
                        << "SHAMapStore: freshen MISS rotation=" << rotationId_.load()
                        << " cache=" << cacheName << " hash=" << key
                        << " bodyInCache=" << (bodyInCache ? 1 : 0) << " type=" << nodeType
                        << " isLeaf=" << leaf << " cowid=" << nodeCowid
                        << " writable=" << writableName_ << " archive=" << archiveName_
                        << (misses == kMaxLoggedPerRotation
                                ? " (further per-node MISS lines suppressed; "
                                  "see FRESHEN_DONE for total)"
                                : "");
                }
            }
            ++check;
            if ((check % progressLogInterval_) == 0u)
            {
                JLOG(journal_.warn())
                    << "SHAMapStore: freshen PROGRESS rotation=" << rotationId_.load()
                    << " cache=" << cacheName << " processed=" << check << " of=" << keys.size()
                    << " misses=" << misses;
            }
            if (!(check % checkHealthInterval_) && healthWait() == HealthResult::Stopping)
            {
                if (logMisses)
                {
                    freshenMissCount_ += misses;
                    freshenMissBodyInCacheCount_ += missesBodyInCache;
                }
                auto const loopMs = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - loopT0);
                JLOG(journal_.warn())
                    << "SHAMapStore: freshen ABORTED rotation=" << rotationId_.load()
                    << " cache=" << cacheName << " processed=" << check << " misses=" << misses
                    << " bodyInCache=" << missesBodyInCache << " loopMs=" << loopMs.count()
                    << " totalMs=" << (loopMs + getKeysMs).count()
                    << (!logMisses ? " (misses expected for this cache; not counted "
                                     "in freshenMisses)"
                                   : "");
                return true;
            }
        }
        if (logMisses)
        {
            freshenMissCount_ += misses;
            freshenMissBodyInCacheCount_ += missesBodyInCache;
        }
        auto const loopMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - loopT0);
        JLOG(journal_.warn()) << "SHAMapStore: freshen END rotation=" << rotationId_.load()
                              << " cache=" << cacheName << " processed=" << check
                              << " misses=" << misses << " bodyInCache=" << missesBodyInCache
                              << " loopMs=" << loopMs.count()
                              << " totalMs=" << (loopMs + getKeysMs).count()
                              << (!logMisses ? " (misses expected for this cache; not counted in "
                                               "freshenMisses)"
                                             : "");
        return false;
    }

    /** delete from sqlite table in batches to not lock the db excessively.
     *  Pause briefly to extend access time to other users.
     *  Call with mutex object unlocked.
     */
    void
    clearSql(
        LedgerIndex lastRotated,
        std::string const& tableName,
        std::function<std::optional<LedgerIndex>()> const& getMinSeq,
        std::function<void(LedgerIndex)> const& deleteBeforeSeq);
    void
    clearCaches(LedgerIndex validatedSeq);
    void
    freshenCaches();
    void
    clearPrior(LedgerIndex lastRotated);

    /**
     * This is a health check for online deletion that waits until xrpld is
     * stable before returning. It returns an indication of whether the server
     * is stopping.
     *
     * @return Whether the server is stopping.
     */
    enum class HealthResult { Stopping, KeepGoing };
    [[nodiscard]] HealthResult
    healthWait();

public:
    void
    start() override
    {
        if (deleteInterval_ != 0u)
            thread_ = std::thread(&SHAMapStoreImp::run, this);
    }

    void
    stop() override;
};

}  // namespace xrpl
