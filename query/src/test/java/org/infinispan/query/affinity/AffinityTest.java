package org.infinispan.query.affinity;

import static java.util.stream.IntStream.rangeClosed;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESDATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_INDEXESMETADATA_CACHENAME;
import static org.infinispan.hibernate.search.spi.InfinispanIntegration.DEFAULT_LOCKING_CACHENAME;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
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
import org.testng.annotations.Test;

/**
 * Test index affinity for the AffinityIndexManager
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.AffinityTest")
@CleanupAfterMethod
public class AffinityTest extends BaseAffinityTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      createClusteredCaches(3, getDefaultCacheConfigBuilder());
   }

   public void testConcurrentWrites() throws InterruptedException {
      int numThreads = 2;
      AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      List<Cache<String, Entity>> cacheList = caches();

      List<Future<?>> futures = rangeClosed(1, numThreads).boxed().map(tid -> {
         return executorService.submit(() -> {
            rangeClosed(1, getNumEntries()).forEach(entry -> {
               int id = counter.incrementAndGet();
               pickCache().put(String.valueOf(id), new Entity(id));
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

      assertEquals(pickCache().size(), numThreads * getNumEntries());
      cacheList.forEach(c -> {
         CacheQuery<Entity> q = Search.getSearchManager(c).getQuery(new MatchAllDocsQuery(), Entity.class);
         eventuallyEquals(numThreads * getNumEntries(), () -> q.list().size());
      });

   }

   public void shouldHaveIndexAffinity() throws Exception {
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
