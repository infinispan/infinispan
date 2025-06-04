package org.infinispan.distribution;

import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
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

   LocalizedCacheTopology createLocalizedCacheTopology(CacheTopology cacheTopology);
}
