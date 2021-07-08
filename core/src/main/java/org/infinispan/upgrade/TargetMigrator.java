package org.infinispan.upgrade;

import org.infinispan.Cache;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.StoreConfiguration;

/**
 * Performs migration operations on the target server or cluster of servers
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public interface TargetMigrator {
   /**
    * Returns the name of this migrator
    */
   String getName();
   /**
    * Performs the synchronization of data between source and target
    */
   long synchronizeData(Cache<Object, Object> cache) throws CacheException;

   /**
    * Performs the synchronization of data between source and target
    */
   long synchronizeData(Cache<Object, Object> cache, int readBatch, int threads) throws CacheException;

   /**
    * Disconnects the target from the source. This operation is the last step that must be performed after a rolling upgrade.
    */
   void disconnectSource(Cache<Object, Object> cache) throws CacheException;

   /**
    * Connects the target cluster to the source cluster through a Remote Store.
    *
    * @param cache The cache to add the store to
    * @param configuration The configuration of the store
    */
   void connectSource(Cache<Object, Object> cache, StoreConfiguration configuration);
}
