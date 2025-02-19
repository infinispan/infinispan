package org.infinispan.configuration.cache;


import static org.infinispan.configuration.parsing.Element.DISTRIBUTED_CACHE;
import static org.infinispan.configuration.parsing.Element.DISTRIBUTED_CACHE_CONFIGURATION;
import static org.infinispan.configuration.parsing.Element.INVALIDATION_CACHE;
import static org.infinispan.configuration.parsing.Element.INVALIDATION_CACHE_CONFIGURATION;
import static org.infinispan.configuration.parsing.Element.LOCAL_CACHE;
import static org.infinispan.configuration.parsing.Element.LOCAL_CACHE_CONFIGURATION;
import static org.infinispan.configuration.parsing.Element.REPLICATED_CACHE;
import static org.infinispan.configuration.parsing.Element.REPLICATED_CACHE_CONFIGURATION;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.protostream.annotations.Proto;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Cache replication mode.
 */
@Proto
@ProtoTypeId(ProtoStreamTypeIds.CACHE_MODE)
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
   DIST_ASYNC;

   private static final CacheMode[] cachedValues = values();

   public static CacheMode valueOf(int order) {
      return cachedValues[order];
   }

   public static CacheMode of(CacheType cacheType, boolean sync) {
      switch (cacheType) {
         case REPLICATION:
            return sync ? REPL_SYNC : REPL_ASYNC;
         case DISTRIBUTION:
            return sync ? DIST_SYNC : DIST_ASYNC;
         case INVALIDATION:
            return sync ? INVALIDATION_SYNC : INVALIDATION_ASYNC;
         case LOCAL:
         default:
            return LOCAL;
      }
   }

   /**
    * Returns true if the mode is invalidation, either sync or async.
    */
   public boolean isInvalidation() {
      return this == INVALIDATION_SYNC || this == INVALIDATION_ASYNC;
   }

   public boolean isSynchronous() {
      return this == REPL_SYNC || this == DIST_SYNC || this == INVALIDATION_SYNC || this == LOCAL;
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

   public boolean needsStateTransfer() {
      return isReplicated() || isDistributed();
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

   public CacheMode toSync(boolean sync) {
      return sync ? toSync() : toAsync();
   }

   public CacheMode toAsync() {
      switch (this) {
         case REPL_SYNC:
            return REPL_ASYNC;
         case INVALIDATION_SYNC:
            return INVALIDATION_ASYNC;
         case DIST_SYNC:
            return DIST_ASYNC;
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
         default:
            return LOCAL_CACHE.getLocalName();
      }
   }

   public Element toElement(boolean template) {
      switch (this) {
         case DIST_SYNC:
         case DIST_ASYNC:
            return template ? DISTRIBUTED_CACHE_CONFIGURATION : DISTRIBUTED_CACHE;
         case REPL_SYNC:
         case REPL_ASYNC:
            return template ? REPLICATED_CACHE_CONFIGURATION : REPLICATED_CACHE;
         case INVALIDATION_SYNC:
         case INVALIDATION_ASYNC:
            return template ? INVALIDATION_CACHE_CONFIGURATION : INVALIDATION_CACHE;
         default:
            return template ? LOCAL_CACHE_CONFIGURATION : LOCAL_CACHE;
      }
   }

   public CacheType cacheType() {
      switch (this) {
         case DIST_ASYNC:
         case DIST_SYNC:
            return CacheType.DISTRIBUTION;
         case REPL_ASYNC:
         case REPL_SYNC:
            return CacheType.REPLICATION;
         case INVALIDATION_ASYNC:
         case INVALIDATION_SYNC:
            return CacheType.INVALIDATION;
         case LOCAL:
         default:
            return CacheType.LOCAL;
      }
   }
}
