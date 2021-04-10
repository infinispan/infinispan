package org.infinispan.server.hotrod;

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import org.infinispan.AdvancedCache;
import org.infinispan.commons.util.BloomFilter;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.container.versioning.VersionGenerator;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.server.core.CacheInfo;

public class HotRodCacheInfo extends CacheInfo<byte[], byte[]> {
   private static final AtomicReferenceFieldUpdater<HotRodCacheInfo, BloomFilter> BLOOM_FILTER_FIELD_UPDATER = AtomicReferenceFieldUpdater.newUpdater(HotRodCacheInfo.class, BloomFilter.class, "bloomFilter");
   final DistributionManager distributionManager;
   final VersionGenerator versionGenerator;
   final Configuration configuration;
   final boolean transactional;
   final boolean clustered;
   volatile boolean indexing;
   volatile BloomFilter<byte[]> bloomFilter;

   HotRodCacheInfo(AdvancedCache<byte[], byte[]> cache, Configuration configuration) {
      super(SecurityActions.anonymizeSecureCache(cache));
      this.distributionManager = SecurityActions.getDistributionManager(cache);
      ComponentRegistry componentRegistry = SecurityActions.getCacheComponentRegistry(cache);
      //Note: HotRod cannot use the same version generator as Optimistic Transaction.
      this.versionGenerator =
            componentRegistry.getComponent(VersionGenerator.class, KnownComponentNames.HOT_ROD_VERSION_GENERATOR);
      this.configuration = configuration;
      this.transactional = configuration.transaction().transactionMode().isTransactional();
      this.clustered = configuration.clustering().cacheMode().isClustered();

      // Start conservative and assume we have all the stuff that can cause operations to block
      this.indexing = true;
   }

   public void update(boolean indexing) {
      this.indexing = indexing;
   }

   public BloomFilter<byte[]> getBloomFilter() {
      return bloomFilter;
   }

   public boolean setBloomFilter(BloomFilter<byte[]> bloomFilter) {
      return BLOOM_FILTER_FIELD_UPDATER.compareAndSet(this, null, bloomFilter);
   }
}
