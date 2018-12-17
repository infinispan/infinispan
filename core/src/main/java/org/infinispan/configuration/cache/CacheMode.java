package org.infinispan.configuration.cache;


import static org.infinispan.configuration.parsing.Element.DISTRIBUTED_CACHE;
import static org.infinispan.configuration.parsing.Element.INVALIDATION_CACHE;
import static org.infinispan.configuration.parsing.Element.LOCAL_CACHE;
import static org.infinispan.configuration.parsing.Element.REPLICATED_CACHE;
import static org.infinispan.configuration.parsing.Element.SCATTERED_CACHE;

import java.util.Arrays;

import org.infinispan.commons.CacheConfigurationException;

/**
 * Cache replication mode.
 */
public enum CacheMode {
   /**
    * Data is not replicated.
    */
   LOCAL,

   /**
    * Data replicated synchronously.
    */
   REPL_SYNC,

   /**
    * Data replicated asynchronously.
    */
   REPL_ASYNC,

   /**
    * Data invalidated synchronously.
    */
   INVALIDATION_SYNC,

   /**
    * Data invalidated asynchronously.
    */
   INVALIDATION_ASYNC,

   /**
    * Synchronous DIST
    */
   DIST_SYNC,

   /**
    * Async DIST
    */
   DIST_ASYNC,

   /**
    * Synchronous scattered cache
    */
   SCATTERED_SYNC;

   private static CacheMode[] cachedValues = values();

   public static CacheMode valueOf(int order) {
      return cachedValues[order];
   }

   /**
    * Returns true if the mode is invalidation, either sync or async.
    */
   public boolean isInvalidation() {
      return this == INVALIDATION_SYNC || this == INVALIDATION_ASYNC;
   }

   public boolean isSynchronous() {
      return this == REPL_SYNC || this == DIST_SYNC || this == INVALIDATION_SYNC || this == SCATTERED_SYNC || this == LOCAL;
   }

   public boolean isClustered() {
      return this != LOCAL;
   }

   public boolean isDistributed() {
      return this == DIST_SYNC || this == DIST_ASYNC;
   }

   public boolean isReplicated() {
      return this == REPL_SYNC || this == REPL_ASYNC;
   }

   public boolean isScattered() { return this == SCATTERED_SYNC; }

   public boolean needsStateTransfer() {
      return isReplicated() || isDistributed() || isScattered();
   }

   public CacheMode toSync() {
      switch (this) {
         case REPL_ASYNC:
            return REPL_SYNC;
         case INVALIDATION_ASYNC:
            return INVALIDATION_SYNC;
         case DIST_ASYNC:
            return DIST_SYNC;
         default:
            return this;
      }
   }

   public CacheMode toAsync() {
      switch (this) {
         case REPL_SYNC:
            return REPL_ASYNC;
         case INVALIDATION_SYNC:
            return INVALIDATION_ASYNC;
         case DIST_SYNC:
            return DIST_ASYNC;
         case SCATTERED_SYNC:
            throw new IllegalArgumentException("Scattered mode does not have asynchronous mode.");
         default:
            return this;
      }
   }

   public String friendlyCacheModeString() {
      switch (this) {
         case REPL_SYNC:
         case REPL_ASYNC:
            return "REPLICATED";
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            return "INVALIDATED";
         case DIST_SYNC:
         case DIST_ASYNC:
            return "DISTRIBUTED";
         case SCATTERED_SYNC:
            return "SCATTERED";
         case LOCAL:
            return "LOCAL";
      }
      throw new IllegalArgumentException("Unknown cache mode " + this);
   }

   public String toCacheType() {
      switch (this) {
         case DIST_SYNC:
         case DIST_ASYNC:
            return DISTRIBUTED_CACHE.getLocalName();
         case REPL_SYNC:
         case REPL_ASYNC:
            return REPLICATED_CACHE.getLocalName();
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            return INVALIDATION_CACHE.getLocalName();
         case SCATTERED_SYNC:
            return SCATTERED_CACHE.getLocalName();
         default:
            return LOCAL_CACHE.getLocalName();
      }
   }

   public static boolean isValidCacheMode(String serializedCacheMode) {
      return Arrays.stream(values()).map(CacheMode::toCacheType).anyMatch(s -> s.equals(serializedCacheMode));
   }

   public static CacheMode fromParts(String distribution, String synchronicity) {
      String sync = synchronicity.toLowerCase();
      if (!sync.equals("sync") && !sync.equals("async"))
         throw new CacheConfigurationException("Invalid cache mode " + distribution + "," + synchronicity);
      switch (distribution.toLowerCase()) {
         case "distributed":
            return sync.equals("sync") ? DIST_SYNC : DIST_ASYNC;
         case "replicated":
            return sync.equals("sync") ? REPL_SYNC : REPL_ASYNC;
         case "local":
            return LOCAL;
         case "scattered":
            return SCATTERED_SYNC;
         case "invalidation":
            return sync.equals("sync") ? INVALIDATION_SYNC : INVALIDATION_ASYNC;
         default:
            throw new CacheConfigurationException("Invalid cache mode " + distribution + "," + synchronicity);
      }
   }
}
