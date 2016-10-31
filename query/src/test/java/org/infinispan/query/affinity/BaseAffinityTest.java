package org.infinispan.query.affinity;

import static java.util.stream.IntStream.rangeClosed;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.configuration.cache.IndexingConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.impl.AffinityPartitioner;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;

public class BaseAffinityTest extends MultipleCacheManagersTest {

   static final int NUM_OWNERS = 2;
   static final int NUM_NODES = 3;
   int ENTRIES = 50;
   protected Random random = new Random();
   protected int REMOTE_TIMEOUT_MINUTES = 3;

   protected Map<String, String> getIndexingProperties() {
      Map<String, String> props = new HashMap<>();
      props.put("hibernate.search.lucene_version", "LUCENE_CURRENT");
      props.put("entity.indexmanager", AffinityIndexManager.class.getName());
      return props;
   }

   protected ConfigurationBuilder getBaseCacheConfig(CacheMode cacheMode) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, false);
      builder.clustering()
            .remoteTimeout(REMOTE_TIMEOUT_MINUTES, TimeUnit.MINUTES)
            .hash().numOwners(getNumOwners())
            .keyPartitioner(new AffinityPartitioner());
      return builder;
   }

   protected ConfigurationBuilder getBaseIndexCacheConfig(CacheMode cacheMode) {
      ConfigurationBuilder builder = getBaseCacheConfig(cacheMode);
      builder.indexing().index(Index.NONE);
      return builder;
   }

   protected Configuration getLockCacheConfig() {
      return getBaseIndexCacheConfig(CacheMode.DIST_SYNC).build();
   }

   protected Configuration getDataCacheConfig() {
      return getBaseIndexCacheConfig(CacheMode.DIST_SYNC).build();
   }

   protected Configuration getMetadataCacheConfig() {
      return getBaseIndexCacheConfig(CacheMode.DIST_SYNC).build();
   }

   protected ConfigurationBuilder getCacheConfig() {
      ConfigurationBuilder configurationBuilder = getBaseCacheConfig(CacheMode.DIST_SYNC);
      IndexingConfigurationBuilder builder = configurationBuilder.indexing()
            .index(Index.PRIMARY_OWNER).addIndexedEntity(Entity.class);
      this.getIndexingProperties().entrySet().forEach(entry -> builder.addProperty(entry.getKey(), entry.getValue()));
      return configurationBuilder;
   }

   protected int getNumOwners() {
      return NUM_OWNERS;
   }

   protected int getNumNodes() {
      return NUM_NODES;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(getNumNodes(), getCacheConfig());
      cacheManagers.forEach(
            cm -> {
               cm.defineConfiguration(DEFAULT_LOCKING_CACHENAME, getLockCacheConfig());
               cm.defineConfiguration(DEFAULT_INDEXESMETADATA_CACHENAME, getMetadataCacheConfig());
               cm.defineConfiguration(DEFAULT_INDEXESDATA_CACHENAME, getDataCacheConfig());
            }
      );
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
      addClusterEnabledCacheManager(getCacheConfig());
      waitForClusterToForm();
   }

}
