package org.infinispan.configuration.cache;


import static org.infinispan.configuration.parsing.Element.DISTRIBUTED_CACHE;
import static org.infinispan.configuration.parsing.Element.DISTRIBUTED_CACHE_CONFIGURATION;
import static org.infinispan.configuration.parsing.Element.INVALIDATION_CACHE;
import static org.infinispan.configuration.parsing.Element.INVALIDATION_CACHE_CONFIGURATION;
import static org.infinispan.configuration.parsing.Element.LOCAL_CACHE;
import static org.infinispan.configuration.parsing.Element.LOCAL_CACHE_CONFIGURATION;
import static org.infinispan.configuration.parsing.Element.REPLICATED_CACHE;
import static org.infinispan.configuration.parsing.Element.REPLICATED_CACHE_CONFIGURATION;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.parsing.Element;
import org.infinispan.protostream.annotations.ProtoEnumValue;

/**
 * Cache replication mode.
 */
public enum CacheMode {
   /**
    * Data is not replicated.
    */
   @ProtoEnumValue(number = 0)
   LOCAL,

   /**
    * Data replicated synchronously.
    */
   @ProtoEnumValue(number = 1)
   REPL_SYNC,

   /**
    * Data replicated asynchronously.
    */
   @ProtoEnumValue(number = 2)
   REPL_ASYNC,

   /**
    * Data invalidated synchronously.
    */
   @ProtoEnumValue(number = 3)
   INVALIDATION_SYNC,

   /**
    * Data invalidated asynchronously.
    */
   @ProtoEnumValue(number = 4)
   INVALIDATION_ASYNC,

   /**
    * Synchronous DIST
    */
   @ProtoEnumValue(number = 5)
   DIST_SYNC,

   /**
    * Async DIST
    */
   @ProtoEnumValue(number = 6)
   DIST_ASYNC;

   private static final CacheMode[] cachedValues = values();

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
         case "invalidation":
            return sync.equals("sync") ? INVALIDATION_SYNC : INVALIDATION_ASYNC;
         default:
            throw new CacheConfigurationException("Invalid cache mode " + distribution + "," + synchronicity);
      }
   }
}
