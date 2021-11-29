package org.infinispan.stats;

/**
 * Similar to {@link Stats} but cluster wide.
 *
 * @author Vladimir Blagojevic
 * @since 7.1
 */
public interface ClusterCacheStats extends Stats, ClusterStats {

   String OBJECT_NAME = "ClusterCacheStats";

   /**
    * @return cluster wide read/writes ratio for the cache
    */
   double getReadWriteRatio();

   /**
    * @return cluster wide total percentage hit/(hit+miss) ratio for this cache
    */
   double getHitRatio();

   /**
    * @return the total number of exclusive locks available in the cluster
    */
   int getNumberOfLocksAvailable();

   /**
    * @return the total number of exclusive locks held in the cluster
    */
   int getNumberOfLocksHeld();

   /**
    * @return the total number of invalidations in the cluster
    */
   long getInvalidations();

   /**
    * @return the total number of activations in the cluster
    */
   long getActivations();

   /**
    * @return the total number of passivations in the cluster
    */
   long getPassivations();

   /**
    * @return the total number of persistence load operations in the cluster
    */
   long getCacheLoaderLoads();

   /**
    * @return the total number of persistence misses in the cluster
    */
   long getCacheLoaderMisses();

   /**
    * @return the total number of persistence store operations in the cluster
    */
   long getStoreWrites();

   /**
    * @return the approximate number of entries.
    *
    * Each owner's copy is counted separately, except entries
    * in shared stores.
    */
   @Override
   long getApproximateEntries();

   /**
    * @return the approximate number of entries in memory.
    *
    * Each owner's copy is counted separately.
    */
   @Override
   long getApproximateEntriesInMemory();

   /**
    * @return the approximate number of unique entries.
    */
   @Override
   long getApproximateEntriesUnique();
}
