package org.infinispan.query.affinity;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.util.stream.IntStream.rangeClosed;
import static org.testng.Assert.assertEquals;

/**
 * Test index affinity for the AffinityIndexManager
 *
 * @author gustavonalle
 * @since 8.2
 */
@Test(groups = "functional", testName = "query.AffinityTest")
public class AffinityTest extends BaseAffinityTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      createClusteredCaches(3, cacheCfg);
   }

   public void testConcurrentWrites() throws InterruptedException {
      int numThreads = 2;
      AtomicInteger counter = new AtomicInteger(0);
      ExecutorService executorService = Executors.newFixedThreadPool(numThreads);
      final List<Cache<String, Entity>> cacheList = caches();

      List<Future<?>> futures = rangeClosed(1, numThreads).boxed().map(tid -> {
         return executorService.submit(() -> {
            rangeClosed(1, ENTRIES).forEach(entry -> {
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

      assertEquals(pickCache().size(), numThreads * ENTRIES);
      cacheList.forEach(c -> {
         CacheQuery q = Search.getSearchManager(c).getQuery(new MatchAllDocsQuery(), Entity.class);
         eventually(() -> q.list().size() == numThreads * ENTRIES);
      });

   }

   public void shouldHaveIndexAffinity() throws Exception {
      populate(1, ENTRIES / 2);
      checkAffinity();

      addNode();
      populate(ENTRIES / 2 + 1, ENTRIES);
      checkAffinity();

      CacheQuery q = Search.getSearchManager(pickCache()).getQuery(new MatchAllDocsQuery(), Entity.class);
      assertEquals(ENTRIES, pickCache().size());
      assertEquals(ENTRIES, q.list().size());

      addNode();
      checkAffinity();
      assertEquals(ENTRIES, pickCache().size());

      populate(ENTRIES + 1, ENTRIES * 2);
      checkAffinity();
      assertEquals(ENTRIES * 2, q.list().size());

   }

}
