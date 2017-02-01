package org.infinispan.distribution;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.remoting.transport.Address;
import org.infinispan.topology.CacheTopology;

/**
 * A component that manages the distribution of elements across a cache cluster
 *
 * @author Manik Surtani
 * @author Mircea.Markus@jboss.com
 * @author Vladimir Blagojevic
 * @author anistor@redhat.com
 * @since 4.0
 */
@Scope(Scopes.NAMED_CACHE)
public interface DistributionManager {

   /**
    * Returns the data locality characteristics of a given key.
    * @param key key to test
    * @return a DataLocality that allows you to test whether a key is mapped to the local node or not, and the degree of
    * certainty of such a result.
    *
    * @deprecated Since 9.0, please use {@code getCacheTopology().getDistributionInfo(key)} instead.
    */
   @Deprecated
   DataLocality getLocality(Object key);

   /**
    * Locates a key in a cluster.  The returned addresses <i>may not</i> be owners of the keys if a rehash happens to be
    * in progress or is pending, so when querying these servers, invalid responses should be checked for and the next
    * address checked accordingly.
    *
    * @param key key to test
    * @return a list of addresses where the key may reside
    *
    * @deprecated Since 9.0, please use {@code getCacheTopology().getDistributionInfo(key)} instead.
    */
   @Deprecated
   List<Address> locate(Object key);

   /**
    * Returns the first Address containing the key.  Equivalent to returning the first element of {@link #locate(Object)}
    * @param key key to test
    * @return the first address on which the key may reside
    * @deprecated Since 9.0, please use {@code getCacheTopology().getDistributionInfo(key)} instead.
    */
   @Deprecated
   Address getPrimaryLocation(Object key);

   /**
    * Locates a list of keys in a cluster.  Like {@link #locate(Object)} the returned addresses <i>may not</i> be owners
    * of the keys if a rehash happens to be in progress or is pending, so when querying these servers, invalid responses
    * should be checked for and the next address checked accordingly.
    *
    * @param keys list of keys to locate
    * @return all the nodes that would need to write a copy of one of the keys.
    * @deprecated Since 9.0, no direct replacement.
    */
   @Deprecated
   Set<Address> locateAll(Collection<Object> keys);

   /**
    * @return the consistent hash used for writing.
    *
    * @deprecated Since 9.0, please use {@link #getWriteConsistentHash()} instead.
    */
   @Deprecated
   default ConsistentHash getConsistentHash() {
      return getWriteConsistentHash();
   }

   /**
    * @return the consistent hash used for reading.
    */
   ConsistentHash getReadConsistentHash();

   /**
    * @return the consistent hash used for writing.
    */
   ConsistentHash getWriteConsistentHash();

   /**
    * Tests whether a given key is affected by a rehash that may be in progress.  If no rehash is in progress, this method
    * returns false.  Helps determine whether additional steps are necessary in handling an operation with a given key.
    *
    * @param key key to test
    * @return whether a key is affected by a rehash
    */
   boolean isAffectedByRehash(Object key);

   /**
    * Tests whether a rehash is in progress
    * @return true if a rehash is in progress, false otherwise
    */
   boolean isRehashInProgress();

   /**
    * Tests whether the current instance has completed joining the cluster
    * @return true if join is in progress, false otherwise
    */
   boolean isJoinComplete();

   /**
    * @return the current cache topology, which includes the read and write consistent hashes.
    */
   LocalizedCacheTopology getCacheTopology();

   /**
    * @deprecated Internal only.
    */
   @Deprecated
   void setCacheTopology(CacheTopology cacheTopology);

   LocalizedCacheTopology createLocalizedCacheTopology(CacheTopology cacheTopology);
}
