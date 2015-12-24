package org.infinispan.query.affinity;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
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
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.backend.QueryInterceptor;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.io.Serializable;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.rangeClosed;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

/**
 * Test index affinity for the ShardIndexManager
 *
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.AffinityTest")
public class AffinityTest extends MultipleCacheManagersTest {

   private static final int ENTRIES = 50;

   private ConfigurationBuilder cacheCfg;
   private Random random = new Random();

   @Override
   protected void createCacheManagers() throws Throwable {
      cacheCfg = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      cacheCfg.clustering().hash().numSegments(60).numOwners(2).keyPartitioner(new AffinityPartitioner());
      cacheCfg.indexing()
              .index(Index.ALL)
              .addProperty("hibernate.search.default.directory_provider", "infinispan")
              .addProperty("hibernate.search.lucene_version", "LUCENE_CURRENT")
              .addProperty("entity.indexmanager", "org.infinispan.query.affinity.ShardIndexManager");
      createClusteredCaches(3, cacheCfg);
      caches().forEach(c -> c.getAdvancedCache().getComponentRegistry().getComponent(QueryInterceptor.class).enableClasses(new Class[]{Entity.class}));
   }

   public void testConcurrentWrites() throws InterruptedException {
      int numThreads = 2;
      AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      final List<Cache<String, Entity>> cacheList = caches();

      List<Future<?>> futures = rangeClosed(1, numThreads).boxed().map(tid -> {
         return executorService.submit(() -> {
            rangeClosed(1, ENTRIES).boxed().forEach(entry -> {
               int id = counter.incrementAndGet();
               pickCache(cacheList).put(String.valueOf(id), new Entity(id));
            });
         });
      }).collect(Collectors.toList());

      futures.forEach(f -> {
         try {
            f.get();
         } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
         }
      });

      assertEquals(pickCache(cacheList).size(), numThreads * ENTRIES);
      cacheList.forEach(c -> {
         CacheQuery q = Search.getSearchManager(c).getQuery(new MatchAllDocsQuery(), Entity.class);
         eventually(() -> q.list().size() == numThreads * ENTRIES);
      });

   }


   public Cache<String, Entity> pickCache(List<Cache<String, Entity>> caches) {
      return caches.get(random.nextInt(caches.size() - 1));
   }

   private void populate(int initialId, int finalId, List<Cache<String, Entity>> caches) {
      rangeClosed(initialId, finalId).boxed().forEach(i -> pickCache(caches).put(i.toString(), new Entity(i)));
   }

   private void addNode() {
      addClusterEnabledCacheManager(cacheCfg);
      waitForClusterToForm();
   }

   public void shouldHaveIndexAffinity() throws Exception {
      List<Cache<String, Entity>> initialCaches = caches();

      populate(1, ENTRIES / 2, initialCaches);
      checkAffinity();

      addNode();
      final List<Cache<String, Entity>> currentCaches = caches();
      populate(ENTRIES / 2 + 1, ENTRIES, currentCaches);
      checkAffinity();

      CacheQuery q = Search.getSearchManager(pickCache(currentCaches)).getQuery(new MatchAllDocsQuery(), Entity.class);
      assertEquals(ENTRIES, pickCache(currentCaches).size());
      assertEquals(ENTRIES, q.list().size());

      addNode();
      checkAffinity();
      assertEquals(ENTRIES, pickCache(caches()).size());

      populate(ENTRIES + 1, ENTRIES * 2, currentCaches);
      checkAffinity();
      assertEquals(ENTRIES * 2, q.list().size());

   }


   private void checkAffinity() {
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

   @AfterMethod
   @Override
   protected void clearContent() throws Throwable {
      cacheManagers.get(0).getCache().clear();
   }

   @Indexed(index = "entity")
   @SuppressWarnings("unused")
   static class Entity implements Serializable {

      @Field
      private final int val;

      public Entity(int val) {
         this.val = val;
      }
   }

}