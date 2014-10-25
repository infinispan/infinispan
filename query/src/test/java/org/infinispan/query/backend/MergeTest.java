package org.infinispan.query.backend;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.test.Person;
import org.infinispan.test.MultipleCacheManagersTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

import static org.infinispan.query.helper.StaticTestingErrorHandler.assertAllGood;
import static org.infinispan.test.TestingUtil.killCacheManagers;

/**
 * Test to simulate concurrent index writing and merges using Infinispan Directory under
 * the InfinispanIndexManager
 *
 * @author gustavonalle
 * @since 7.0
 */
@Test(groups = "profiling", testName = "query.backend.MergeTest")
public class MergeTest extends MultipleCacheManagersTest {

   // Low merge factor means more frequent merges
   private static final String MERGE_FACTOR = "10";
   private static final int OBJECT_COUNT = 100000;
   private static final int NUMBER_OF_THREADS = 10;
   private static final boolean TX_ENABLED = false;

   Cache<Long, Person> cache1, cache2;

   /**
    * Main method to escape from testng
    */
   public static void main(String[] args) throws Throwable {
      MergeTest c = new MergeTest();
      c.createBeforeClass();
      c.createBeforeMethod();
      c.testMergesWrites();
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, TX_ENABLED);
      cacheCfg.clustering().sync().replTimeout(120000)
            .indexing().index(Index.LOCAL)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.indexmanager", "org.infinispan.query.indexmanager.InfinispanIndexManager")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("default.indexwriter.merge_factor", MERGE_FACTOR)
            .addProperty("hibernate.search.default.worker.execution", "async")
            .addProperty("default.indexwriter.merge_max_size", "1024")
            .addProperty("default.indexwriter.ram_buffer_size", "256")
      ;
      List<Cache<Long, Person>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
   }

   public void testMergesWrites() throws Exception {
      final long start = System.currentTimeMillis();

      ExecutorService executorService = Executors.newCachedThreadPool();
      final CountDownLatch waitFor = new CountDownLatch(1);
      final AtomicLong id = new AtomicLong(1);
      ArrayList<Future> futures = new ArrayList<>();
      final Random random = new Random();

      for (int i = 0; i < NUMBER_OF_THREADS; i++) {
         futures.add(executorService.submit(new Runnable() {
            @Override
            public void run() {
               try {
                  waitFor.await();
                  Thread.sleep(random.nextInt(3000));
                  for (int j = 0; j < OBJECT_COUNT; j++) {
                     Cache<Long, Person> cache = (j % 2 == 0) ? cache1 : cache2;
                     long key = id.incrementAndGet();
                     cache.put(key, new Person("name" + key, "blurb", 30));
                     if (j % 100 == 0) {
                        System.out.println(j + " in " + (System.currentTimeMillis() - start) / 1000 + "s ");
                     }
                  }
               } catch (InterruptedException e) {
                  e.printStackTrace();
               }
            }
         }));
      }

      waitFor.countDown();
      for (Future f : futures) {
         f.get();
      }

      assertAllGood(cache1, cache2);
      System.out.println("Load took: " + (System.currentTimeMillis() - start) / 1000 + " s");
      SearchManager searchManager = Search.getSearchManager(cache1);
      final CacheQuery query = searchManager.getQuery(new MatchAllDocsQuery(), Person.class);
      final int total = NUMBER_OF_THREADS * OBJECT_COUNT;
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return query.list().size() == total;
         }
      });
      System.out.println("Indexing finished: " + query.list().size());
      killCacheManagers(cacheManagers);
   }
}
