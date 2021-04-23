package org.infinispan.xsite.irac;

import java.util.Collection;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.LongAdder;

import org.infinispan.commons.util.IntSet;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.metadata.impl.IracMetadata;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.concurrent.CompletableFutures;
import org.infinispan.xsite.statetransfer.XSiteState;

/**
 * A no-op implementation of {@link IracManager} for cache without asynchronous
 * remote site backups.
 *
 * @author Pedro Ruivo
 * @since 11.0
 */
@Scope(Scopes.NAMED_CACHE)
public class NoOpIracManager implements IracManager {

    private static final NoOpIracManager INSTANCE = new NoOpIracManager();

    private final LongAdder conflictsCounts = new LongAdder();
    private final LongAdder discardCounts = new LongAdder();
    private final LongAdder numberOfConflictLocalWins = new LongAdder();
    private final LongAdder numberOfConflictRemoteWins = new LongAdder();
    private final LongAdder numberOfConflictMerged = new LongAdder();

    private NoOpIracManager() {
    }

    public static NoOpIracManager getInstance() {
        return INSTANCE;
    }

    @Override
    public void trackUpdatedKey(int segment, Object key, Object lockOwner) {
        // no-op
    }

    @Override
    public CompletionStage<Void> trackForStateTransfer(Collection<XSiteState> stateList) {
        return CompletableFutures.completedNull();
    }

    @Override
    public void trackClear() {
        // no-op
    }

    @Override
    public void cleanupKey(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
        // no-op
    }

    @Override
    public void onTopologyUpdate(CacheTopology oldCacheTopology, CacheTopology newCacheTopology) {
        // no-op
    }

    @Override
    public void requestState(Address origin, IntSet segments) {
        // no-op
    }

    @Override
    public void receiveState(int segment, Object key, Object lockOwner, IracMetadata tombstone) {
        // no-op
    }

    @Override
    public CompletionStage<Boolean> checkAndTrackExpiration(Object key) {
        return CompletableFutures.completedTrue();
    }

    @Override
    public void incrementDiscards() {
        discardCounts.increment();
    }

    @Override
    public void incrementConflicts() {
        conflictsCounts.increment();
    }

    @Override
    public void increaseNumberOfConflictLocalWins() {
        numberOfConflictLocalWins.increment();
    }

    @Override
    public void increaseNumberOfConflictRemoteWins() {
        numberOfConflictRemoteWins.increment();
    }

    @Override
    public void increaseNumberOfConflictMerged() {
        numberOfConflictMerged.increment();
    }
}
