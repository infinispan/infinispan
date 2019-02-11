package org.infinispan.conflict;

import java.util.Map;
import java.util.stream.Stream;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;

/**
 * @author Ryan Emerson
 * @since 9.1
 */
@Scope(Scopes.NAMED_CACHE)
public interface ConflictManager<K, V> {

   /**
    * Get all CacheEntry's that exists for a given key. Note, concurrent calls to this method for the same key will utilise
    * the same CompletableFuture inside this method and consequently return the same results as all other invocations.
    * If this method is invoked during state transfer it will block until rehashing has completed.  Similarly, if
    * state transfer is initiated during an invocation of this method and rehashing affects the segments of the provided
    * key, the initial requests for the entries versions are cancelled and re-attempted once state transfer has completed.
    *
    * This method utilises the addresses of the local {@link DistributionInfo#writeOwners()} to request values for a given key.
    * If a value does not exist for a key at one of the addresses, then a null valued is mapped to said address.
    *
    * @param key the key for which associated entries are to be returned
    * @return a map of an address and it's associated CacheEntry
    * @throws org.infinispan.commons.CacheException if one or more versions of a key cannot be retrieved.
    */
   Map<Address, InternalCacheValue<V>> getAllVersions(K key);

   /**
    * Returns a stream of conflicts detected in the cluster. This is a lazily-loaded stream which searches for conflicts
    * by sequentially fetching cache segments from their respective owner nodes.  If a rebalance is initiated whilst the
    * stream is fetching a cache segment, then a CacheException is thrown when executing the stream.
    *
    * @return a stream of Map&lt;Address, CacheEntry&gt; for all conflicts detected throughout this cache.
    * @throws IllegalStateException if called whilst a previous conflicts stream is still executing or state transfer is in progress.
    */
   Stream<Map<Address, CacheEntry<K, V>>> getConflicts();

   /**
    * Utilises {@link ConflictManager#getConflicts()} to discover conflicts between Key replicas and utilises the configured
    * {@link EntryMergePolicy} to determine which entry should take precedence. The
    * resulting {@link org.infinispan.container.entries.CacheEntry} is then applied on all replicas in the cluster.
    */
   void resolveConflicts();

   /**
    * Utilises {@link ConflictManager#getConflicts()} to discover conflicts between Key replicas and utilises the provided
    * {@link EntryMergePolicy} to determine which entry should take precedence. The
    * resulting {@link org.infinispan.container.entries.CacheEntry} is then applied on all replicas in the cluster.
    * @param mergePolicy the policy to be applied to all detected conflicts
    */
   void resolveConflicts(EntryMergePolicy<K, V> mergePolicy);

   /**
    * @return true if a state transfer is currently in progress.
    */
   boolean isStateTransferInProgress();

   /**
    * @return true if conflict resolution is in progress. This can happen if the user has multiple threads interacting
    * with the ConflictManager or if a Split-brain merge is in progress.
    */
   boolean isConflictResolutionInProgress();
}
