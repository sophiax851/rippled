#pragma once

#include <xrpl/nodestore/Database.h>

namespace xrpl::NodeStore {

/* This class has two key-value store Backend objects for persisting SHAMap
 * records. This facilitates online deletion of data. New backends are
 * rotated in. Old ones are rotated out and deleted.
 */

class DatabaseRotating : public Database
{
public:
    DatabaseRotating(
        Scheduler& scheduler,
        int readThreads,
        Section const& config,
        beast::Journal journal)
        : Database(scheduler, readThreads, config, journal)
    {
    }

    /** Rotates the backends.

        @param newBackend New writable backend
        @param f A function executed after the rotation outside of lock. The
       values passed to f will be the new backend database names _after_
       rotation.
    */
    virtual void
    rotate(
        std::unique_ptr<NodeStore::Backend>&& newBackend,
        std::function<void(std::string const& writableName, std::string const& archiveName)> const&
            f) = 0;

    /** Returns the current (writable, archive) backend names under lock. */
    virtual std::pair<std::string, std::string>
    getBackendNames() const = 0;

    /** Marks a rotation as in flight (or finished).

        While set, an ordinary (duplicate=false) fetch that is served by the
        archive backend is also stored into the writable backend. The archive
        is about to be deleted; without this, a node body read from it after
        online delete's cache-freshen snapshot would survive only in RAM and
        lose its last disk copy at the swap. Set by SHAMapStore just before
        freshening caches and cleared once rotate() completes (or the
        rotation aborts).
    */
    virtual void
    setRotationInFlight(bool inFlight) = 0;
};

}  // namespace xrpl::NodeStore
