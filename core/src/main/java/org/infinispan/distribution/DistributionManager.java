package org.infinispan.distribution;

import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.loaders.CacheStore;
import org.infinispan.remoting.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * A component that manages the distribution of elements across a cache cluster
 *
 * @author Manik Surtani
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DistributionManager {

   boolean isLocal(Object key);

   /**
    * Locates a key in a cluster.  The returned addresses <i>may not</i> be owners of the keys if a rehash happens to be
    * in progress or is pending, so when querying these servers, invalid responses should be checked for and the next
    * address checked accordingly.
    *
    * @param key key to test
    * @return a list of addresses where the key may reside
    */
   List<Address> locate(Object key);

   /**
    * Locates a list of keys in a cluster.  Like {@link #locate(Object)} the returned addresses <i>may not</i> be owners
    * of the keys if a rehash happens to be in progress or is pending, so when querying these servers, invalid responses
    * should be checked for and the next address checked accordingly.
    *
    * @param keys list of keys to test
    * @return a list of addresses where the key may reside
    */
   Map<Object, List<Address>> locateAll(Collection<Object> keys);

   /**
    * Transforms a cache entry so it is marked for L1 rather than the primary cache data structure.
    *
    * @param entry entry to transform
    */
   void transformForL1(CacheEntry entry);

   /**
    * Retrieves a cache entry from a remote source
    *
    * @param key key to look up
    * @return an internal cache entry, or null if it cannot be located
    */
   InternalCacheEntry retrieveFromRemoteSource(Object key) throws Exception;

   ConsistentHash getConsistentHash();

   void setConsistentHash(ConsistentHash consistentHash);

   /**
    * Tests whether a given key is affected by a rehash that may be in progress.
    *
    * @param key key to test
    * @return whether a key is affected by a rehash
    */
   boolean isAffectedByRehash(Object key);

   TransactionLogger getTransactionLogger();

   /**
    * "Asks" a coordinator if a joiner may join.  Used to serialize joins such that only a single joiner comes in at any
    * given time.
    *
    * @param joiner joiner who wants to join
    * @return a consistent hash prior to the joiner joining (if the joiner is allowed to join), otherwise null.
    */
   List<Address> requestPermissionToJoin(Address joiner);

   /**
    * Notifies a coordinator when a join completes
    *
    * @param joiner joiner who has completed.
    */
   void notifyJoinComplete(Address joiner);

   /**
    * This will cause all nodes to add the joiner to their UnionCH
    *
    * @param joiner
    * @param starting
    */
   void informRehashOnJoin(Address joiner, boolean starting);

   /**
    * Retrieves a cache store if one is available and set up for use in rehashing.  May return null!
    *
    * @return a cache store is one is available and configured for use in rehashing, or null otherwise.
    */
   CacheStore getCacheStoreForRehashing();

   boolean isRehashInProgress();

   boolean isJoinComplete();

   void applyReceivedState(Map<Object, InternalCacheValue> state);
}

