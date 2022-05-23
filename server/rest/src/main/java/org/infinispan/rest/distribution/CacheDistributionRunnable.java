package org.infinispan.rest.distribution;

import static org.infinispan.commons.marshall.ProtoStreamTypeIds.CACHE_DISTRIBUTION_RUNNABLE;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;
import org.infinispan.util.function.SerializableFunction;

@ProtoTypeId(CACHE_DISTRIBUTION_RUNNABLE)
public final class CacheDistributionRunnable implements SerializableFunction<EmbeddedCacheManager, CacheDistributionInfo> {
   private final String cacheName;

   @ProtoFactory
   public CacheDistributionRunnable(String cacheName) {
      this.cacheName = cacheName;
   }

   @Override
   public CacheDistributionInfo apply(EmbeddedCacheManager manager) {
      Cache<?, ?> cache = SecurityActions.getCache(cacheName, manager);
      return CacheDistributionInfo.resolve(cache.getAdvancedCache());
   }

   @ProtoField(1)
   public String cacheName() {
      return cacheName;
   }
}
