package org.infinispan.statetransfer;

import java.util.Objects;

import org.infinispan.configuration.cache.CacheMode;

public enum RebalanceType {
   /**
    * Used by local and invalidation cache modes. No state transfer is happening.
    */
   NONE,
   /**
    * Used by distributed and replicated caches. To guarantee consistent results and non-blocking reads,
    * cache must undergo a series of 4 topology changes:
    * STABLE &rarr; READ_OLD_WRITE_ALL &rarr; READ_ALL_WRITE_ALL &rarr; READ_NEW_WRITE_ALL &rarr; STABLE
    */
   FOUR_PHASE;

   public static RebalanceType from(CacheMode cacheMode) {
      switch (Objects.requireNonNull(cacheMode)) {
         case LOCAL:
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            return NONE;
         case REPL_SYNC:
         case REPL_ASYNC:
         case DIST_SYNC:
         case DIST_ASYNC:
            return FOUR_PHASE;
         default:
            throw new IllegalArgumentException();
      }
   }
}
