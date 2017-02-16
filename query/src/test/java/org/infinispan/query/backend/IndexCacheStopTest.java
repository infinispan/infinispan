package org.infinispan.query.backend;

import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertEquals;

import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.lifecycle.ComponentStatus;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.Person;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.CyclicDependencyException;
import org.infinispan.util.DependencyGraph;
import org.testng.annotations.Test;

/**
 * Tests for cache stop order when storing indexes on infinispan
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.backend.IndexCacheStopTest")
public class IndexCacheStopTest extends AbstractInfinispanTest {

   private static final int CACHE_SIZE = 10;

   @Test
   public void testIndexingOnDefaultCache() {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager(getIndexedConfig());
      startAndIndexData(null, cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testIndexingOnNamedCache() {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager(getIndexedConfig());
      cacheManager.defineConfiguration("custom", cacheManager.getDefaultCacheConfiguration());
      startAndIndexData("custom", cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testIndexingOnMultipleCaches() {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager();
      cacheManager.defineConfiguration("cache1", getIndexedConfig().build());
      cacheManager.defineConfiguration("cache2", getIndexedConfigWithCustomCaches("lockCache", "metadataCache", "dataCache").build());
      startAndIndexData("cache1", cacheManager);
      startAndIndexData("cache2", cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testIndexingWithInfinispanIndexManager() {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager();
      cacheManager.defineConfiguration("cache", getIndexedConfigWithInfinispanIndexManager().build());
      startAndIndexData("cache", cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }


   @Test
   public void testIndexingWithCustomLock() throws CyclicDependencyException {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager();
      DependencyGraph<String> graph = TestingUtil.extractField(cacheManager, "cacheDependencyGraph");
      cacheManager.defineConfiguration("cache", getIndexedConfigWithCustomLock().build());
      startAndIndexData("cache", cacheManager);
      cacheManager.stop();

      List<String> cacheOrder = graph.topologicalSort();

      assertTrue(cacheOrder.indexOf("cache") < cacheOrder.indexOf("LuceneIndexesData"));
      assertTrue(cacheOrder.indexOf("cache") < cacheOrder.indexOf("LuceneIndexesMetadata"));
      assertTrue(cacheOrder.indexOf("cache") < cacheOrder.indexOf("LuceneIndexesLocking"));
      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testIndexingOnCacheItself() throws CyclicDependencyException {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager();
      cacheManager.defineConfiguration("single", getIndexedConfigWithCustomCaches("single", "single", "single").build());
      startAndIndexData("single", cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testIndexingMultipleDirectoriesOnSameCache() throws CyclicDependencyException {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager();
      cacheManager.defineConfiguration("cacheA", getIndexedConfigWithCustomCaches("single", "single", "single").build());
      cacheManager.defineConfiguration("cacheB", getIndexedConfigWithCustomCaches("single", "single", "single").build());
      startAndIndexData("cacheA", cacheManager);
      startAndIndexOtherEntityData("cacheB", cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testIndexingHierarchically() throws CyclicDependencyException {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager();
      cacheManager.defineConfiguration("cacheC", getIndexedConfigWithCustomCaches("cacheB", "cacheB", "cacheB").build());
      cacheManager.defineConfiguration("cacheB", getIndexedConfigWithCustomCaches("cacheA", "cacheA", "cacheA").build());
      cacheManager.defineConfiguration("cacheA", getIndexedConfig().build());
      startAndIndexData("cacheA", cacheManager);
      startAndIndexData("cacheB", cacheManager);
      startAndIndexData("cacheC", cacheManager);
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   @Test
   public void testStartAndStopWithEmptyCache() throws CyclicDependencyException {
      EmbeddedCacheManager cacheManager = createClusteredCacheManager(getIndexedConfig());
      cacheManager.getCache();
      cacheManager.stop();

      assertEquals(cacheManager.getStatus(), ComponentStatus.TERMINATED);
   }

   private void startAndIndexData(String cacheName, CacheContainer cacheContainer) {
      startAndIndexEntityData(cacheName, cacheContainer, i -> new Person("name" + i, "blurb" + i, i), Person.class);
   }

   private void startAndIndexOtherEntityData(String cacheName, CacheContainer cacheContainer) {
      startAndIndexEntityData(cacheName, cacheContainer, i -> new AnotherGrassEater("name" + i, "blurb" + i), AnotherGrassEater.class);
   }

   private <T> void startAndIndexEntityData(String cacheName, CacheContainer cacheContainer, Function<Integer, T> provider, Class<T> type) {
      Cache<Integer, T> cache;
      if (cacheName == null) {
         cache = cacheContainer.getCache();
      } else {
         cache = cacheContainer.getCache(cacheName);
      }
      populateData(cache, provider);
      assertIndexPopulated(cache, type);
   }

   private <T> void assertIndexPopulated(Cache<Integer, T> cache, Class<T> clazz) {
      CacheQuery<T> query = Search.getSearchManager(cache).getQuery(new MatchAllDocsQuery(), clazz);
      assertEquals(query.list().size(), CACHE_SIZE);
   }

   private <T> void populateData(Cache<Integer, T> cache, Function<Integer, T> provider) {
      IntStream.range(0, CACHE_SIZE).forEach(i -> cache.put(i, provider.apply(i)));
   }

   private ConfigurationBuilder getBaseConfig() {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.clustering().cacheMode(CacheMode.DIST_SYNC)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).recovery().disable()
            .locking()
            .lockAcquisitionTimeout(10000);
      return cfg;
   }

   private ConfigurationBuilder getIndexedConfig() {
      ConfigurationBuilder cfg = getBaseConfig();
      cfg.indexing().index(Index.ALL)
              .addIndexedEntity(Person.class)
              .addIndexedEntity(AnotherGrassEater.class)
              .addProperty("default.directory_provider", "infinispan")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      return cfg;
   }

   private ConfigurationBuilder getIndexedConfigWithCustomCaches(String lockCache, String metadataCache, String dataCache) {
      ConfigurationBuilder cfg = getIndexedConfig();
      cfg.indexing().index(Index.ALL)
            .addProperty("default.locking_cachename", lockCache)
            .addProperty("default.data_cachename", dataCache)
            .addProperty("default.metadata_cachename", metadataCache)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return cfg;
   }

   private ConfigurationBuilder getIndexedConfigWithInfinispanIndexManager() {
      ConfigurationBuilder cfg = getIndexedConfig();
      cfg.indexing().index(Index.ALL)
              .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      return cfg;
   }

   private ConfigurationBuilder getIndexedConfigWithCustomLock() {
      ConfigurationBuilder cfg = getIndexedConfig();
      cfg.indexing().index(Index.ALL)
              .addProperty("default.locking_strategy", "none")
              .addProperty("lucene_version", "LUCENE_CURRENT");
      return cfg;
   }

}
