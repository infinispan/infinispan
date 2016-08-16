package org.infinispan.query.affinity;

import static java.util.stream.IntStream.rangeClosed;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;

public abstract class BaseAffinityTest extends MultipleCacheManagersTest {

   protected int ENTRIES = 50;
   protected Random random = new Random();
   protected ConfigurationBuilder cacheCfg;

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.clustering().hash().keyPartitioner(new AffinityPartitioner());
      cacheCfg.indexing()
              .index(Index.ALL)
              .addIndexedEntity(Entity.class)
              .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT")
              .addProperty("entity.indexmanager", AffinityIndexManager.class.getName());

   }

   void checkAffinity() {
      for (EmbeddedCacheManager clusterMember : cacheManagers) {
         checkAffinity(clusterMember.getCache(DEFAULT_INDEXESDATA_CACHENAME));
         checkAffinity(clusterMember.getCache(DEFAULT_INDEXESMETADATA_CACHENAME));
         checkAffinity(clusterMember.getCache(DEFAULT_LOCKING_CACHENAME));
      }
   }

   private void checkAffinity(Cache<IndexScopedKey, ?> indexCache) {
      AdvancedCache<IndexScopedKey, ?> advancedCache = indexCache.getAdvancedCache();
      DataContainer<IndexScopedKey, ?> dataContainer = advancedCache.getDataContainer();
      ConsistentHash consistentHash = advancedCache.getDistributionManager().getConsistentHash();
      Address address = advancedCache.getRpcManager().getAddress();
      Set<Integer> ownedSegments = consistentHash.getSegmentsForOwner(address);
      dataContainer.forEach(entry -> {
         int segmentAffinity = entry.getKey().getAffinitySegmentId();
         assertTrue(ownedSegments.contains(segmentAffinity));
      });
   }

   void populate(int initialId, int finalId) {
      rangeClosed(initialId, finalId).forEach(i -> pickCache().put(String.valueOf(i), new Entity(i)));
   }

   synchronized Cache<String, Entity> pickCache() {
      List<Cache<String, Entity>> caches = caches();
      int size = caches.size();
      return caches.get(random.nextInt(size));
   }

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      if (!cacheManagers.isEmpty()) {
         cacheManagers.get(0).getCache().clear();
      }
   }

   synchronized void addNode() {
      addClusterEnabledCacheManager(cacheCfg);
      waitForClusterToForm();
   }

}
