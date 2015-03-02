package org.infinispan.distribution;

import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.topology.CacheTopology;

/**
 * // TODO: Document this
 *
 * @author Pedro Ruivo
 * @since 7.2
 */
public enum LookupMode {
   READ {
      @Override
      public ConsistentHash getConsistentHash(CacheTopology cacheTopology) {
         assertNonNull(cacheTopology);
         return cacheTopology.getReadConsistentHash();
      }
   },
   WRITE {
      @Override
      public ConsistentHash getConsistentHash(CacheTopology cacheTopology) {
         assertNonNull(cacheTopology);
         return cacheTopology.getWriteConsistentHash();
      }
   };

   public abstract ConsistentHash getConsistentHash(CacheTopology cacheTopology);

   private static void assertNonNull(CacheTopology cacheTopology) {
      if (cacheTopology == null) {
         throw new NullPointerException("Cache Topology can't be null.");
      }
   }
}
