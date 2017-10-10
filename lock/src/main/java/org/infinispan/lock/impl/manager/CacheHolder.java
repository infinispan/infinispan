package org.infinispan.lock.impl.manager;

import org.infinispan.AdvancedCache;
import org.infinispan.context.Flag;
import org.infinispan.lock.impl.entries.ClusteredLockKey;
import org.infinispan.lock.impl.entries.ClusteredLockValue;

public class CacheHolder {
   private final AdvancedCache<? extends ClusteredLockKey, ClusteredLockValue> clusteredLockCache;

   public CacheHolder(AdvancedCache<? extends ClusteredLockKey, ClusteredLockValue> clusteredLockCache) {
      this.clusteredLockCache = clusteredLockCache;
   }

   <K extends ClusteredLockKey> AdvancedCache<K, ClusteredLockValue> getClusteredLockCache() {
      //noinspection unchecked
      return (AdvancedCache<K, ClusteredLockValue>) clusteredLockCache.withFlags(Flag.SKIP_CACHE_LOAD, Flag.SKIP_CACHE_STORE);
   }
}
