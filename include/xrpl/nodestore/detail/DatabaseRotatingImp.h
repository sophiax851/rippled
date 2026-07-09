#pragma once

#include <xrpl/nodestore/DatabaseRotating.h>

#include <atomic>
#include <cstdint>
#include <mutex>

namespace xrpl::NodeStore {

class DatabaseRotatingImp : public DatabaseRotating
{
public:
    DatabaseRotatingImp() = delete;
    DatabaseRotatingImp(DatabaseRotatingImp const&) = delete;
    DatabaseRotatingImp&
    operator=(DatabaseRotatingImp const&) = delete;

    DatabaseRotatingImp(
        Scheduler& scheduler,
        int readThreads,
        std::shared_ptr<Backend> writableBackend,
        std::shared_ptr<Backend> archiveBackend,
        Section const& config,
        beast::Journal j);

    ~DatabaseRotatingImp() override
    {
        stop();
    }

    void
    rotate(
        std::unique_ptr<NodeStore::Backend>&& newBackend,
        std::function<void(std::string const& writableName, std::string const& archiveName)> const&
            f) override;

    std::string
    getName() const override;

    std::int32_t
    getWriteLoad() const override;

    void
    importDatabase(Database& source) override;

    bool
    isSameDB(std::uint32_t, std::uint32_t) override
    {
        // rotating store acts as one logical database
        return true;
    }

    void
    store(NodeObjectType type, Blob&& data, uint256 const& hash, std::uint32_t) override;

    void
    sync() override;

    std::pair<std::string, std::string>
    getBackendNames() const override;

    void
    setRotationInFlight(bool inFlight) override;

private:
    std::shared_ptr<Backend> writableBackend_;
    std::shared_ptr<Backend> archiveBackend_;
    mutable std::mutex mutex_;

    // Race detection for rotation vs. concurrent store/fetch. rotationGen_
    // is incremented under mutex_ on every swap so a store/fetch that
    // captured a backend pointer can compare generations before and after
    // and tell whether the pointer it used is still the current writable.
    std::atomic<std::uint64_t> rotationGen_{0};
    std::atomic<std::int64_t> inFlightStores_{0};
    std::atomic<std::int64_t> inFlightFetches_{0};

    // Per-rotation totals used both as throttle keys and as aggregate
    // counters reported in the SWAP log line. Reset under mutex_ in rotate().
    std::atomic<std::uint64_t> storeRaceCount_{0};
    std::atomic<std::uint64_t> fetchRaceCount_{0};
    std::atomic<std::uint64_t> fetchMissCount_{0};
    static constexpr std::uint64_t kMaxLoggedPerRotation = 10;

    // True between SHAMapStore's pre-freshen setRotationInFlight(true) and
    // the completion of rotate(). While true, archive hits on ordinary
    // (duplicate=false) fetches are copied forward into the writable
    // backend; copyForwardCount_ counts those rescues per window and is
    // reset in rotate().
    std::atomic<bool> rotationInFlight_{false};
    std::atomic<std::uint64_t> copyForwardCount_{0};

    std::shared_ptr<NodeObject>
    fetchNodeObject(uint256 const& hash, std::uint32_t, FetchReport& fetchReport, bool duplicate)
        override;

    void
    forEach(std::function<void(std::shared_ptr<NodeObject>)> f) override;
};

}  // namespace xrpl::NodeStore
