package org.infinispan.query.affinity;

import static java.util.stream.IntStream.rangeClosed;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.lucene.IndexScopedKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Test index affinity for the AffinityIndexManager
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.AffinityTest")
@CleanupAfterMethod
public class AffinityTest extends BaseAffinityTest {

   private ExecutorService executorService;

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, getDefaultCacheConfigBuilder());
   }

   @AfterMethod(alwaysRun = true)
   @Override
   protected void clearContent() throws Throwable {
      if (executorService != null) {
         executorService.shutdownNow();
         executorService = null;
      }
      super.clearContent();
   }

   public void testConcurrentWrites() throws Exception {
      int numThreads = 2;
      AtomicInteger counter = new AtomicInteger(0);
      executorService = Executors.newFixedThreadPool(numThreads);
      List<Cache<String, Entity>> cacheList = caches();

      log.info("Starting threads");
      List<Future<?>> futures = rangeClosed(1, numThreads).boxed().map(tid -> executorService.submit(() ->
            rangeClosed(1, getNumEntries()).forEach(entry -> {
               int id = counter.incrementAndGet();
               pickCache().put(String.valueOf(id), new Entity(id));
            }))).collect(Collectors.toList());

      log.info("Waiting for threads");
      for (Future<?> f : futures) {
         f.get();
      }

      log.info("Checking cache size");
      cacheList.forEach(c -> {
         assertEquals(c.size(), numThreads * getNumEntries());
         CacheQuery<Entity> q = Search.getSearchManager(c).getQuery(new MatchAllDocsQuery(), Entity.class);
         eventuallyEquals(numThreads * getNumEntries(), () -> q.list().size());
      });

   }

   @Test(enabled = false,description = "ISPN-9825")
   public void shouldHaveIndexAffinity() {
      populate(1, getNumEntries() / 2);
      checkAffinity();

      addNode();
      populate(getNumEntries() / 2 + 1, getNumEntries());
      checkAffinity();

      CacheQuery<Entity> q = Search.getSearchManager(pickCache()).getQuery(new MatchAllDocsQuery(), Entity.class);
      assertEquals(getNumEntries(), pickCache().size());
      assertEquals(getNumEntries(), q.list().size());

      addNode();
      checkAffinity();
      assertEquals(getNumEntries(), pickCache().size());

      populate(getNumEntries() + 1, getNumEntries() * 2);
      checkAffinity();
      assertEquals(getNumEntries() * 2, q.list().size());
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

}
