package org.infinispan.query.distributed;

import static org.infinispan.configuration.cache.IndexStorage.LOCAL_HEAP;
import static org.infinispan.test.fwk.TestCacheManagerFactory.getDefaultCacheConfiguration;

import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commons.api.query.Query;
import org.infinispan.commons.test.TestResourceTracker;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.Indexer;
import org.infinispan.query.Search;
import org.infinispan.query.impl.ComponentRegistryUtils;
import org.infinispan.query.impl.massindex.IndexUpdater;
import org.infinispan.query.test.QueryTestSCI;
import org.infinispan.query.test.Transaction;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;

/**
 * Long running test for the async MassIndexer, specially regarding cancellation. Supposed to be run as a main class.
 * @author gustavonalle
 * @since 7.1
 */
public class AsyncMassIndexPerfTest extends MultipleCacheManagersTest {

   /**
    * Number of entries to write
    */
   private static final int OBJECT_COUNT = 1_000_000;

   /**
    * Number of threads to do the initial load
    */
   private static final int WRITING_THREADS = 5;

   /**
    * If should SKIP_INDEX during initial load
    */
   private static final boolean DISABLE_INDEX_WHEN_INSERTING = true;

   private static final boolean TX_ENABLED = false;
   private static final String MERGE_FACTOR = "30";
   private static final CacheMode CACHE_MODE = CacheMode.LOCAL;
   private static final IndexStorage INDEX_STORAGE = LOCAL_HEAP;
   /**
    * For status report during insertion
    */
   private static final int PRINT_EACH = 10;

   private Cache<Integer, Transaction> cache1, cache2;
   private Indexer indexer;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg;
      boolean local = CACHE_MODE == CacheMode.LOCAL;
      if (local) {
         cacheCfg = getDefaultCacheConfiguration(TX_ENABLED);
      } else {
         cacheCfg = getDefaultClusteredCacheConfig(CACHE_MODE, TX_ENABLED);
         cacheCfg.clustering().remoteTimeout(120000);
      }
      cacheCfg.indexing().enable()
            .storage(INDEX_STORAGE)
            .addIndexedEntity(Transaction.class)
            .writer().merge().factor(Integer.parseInt(MERGE_FACTOR));

      if (!local) {
         createClusteredCaches(2, QueryTestSCI.INSTANCE, cacheCfg);
         cache2 = cache(1);
         cache1 = cache(0);
      } else {
         EmbeddedCacheManager cacheManager = TestCacheManagerFactory.createCacheManager(cacheCfg);
         cache1 = cacheManager.getCache();
         cache2 = cacheManager.getCache();
      }
      indexer = Search.getIndexer(cache1);
   }

   private void writeData() throws InterruptedException {
      ExecutorService executorService = Executors.newFixedThreadPool(WRITING_THREADS, getTestThreadFactory("Worker"));
      final AtomicInteger counter = new AtomicInteger(0);
      for (int i = 0; i < OBJECT_COUNT; i++) {
         executorService.submit(() -> {
            Cache<Integer, Transaction> insertCache = cache1;
            if (DISABLE_INDEX_WHEN_INSERTING) {
               insertCache = insertCache.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
            }
            int key = counter.incrementAndGet();
            insertCache.put(key, new Transaction(key * 100, "0eab" + key));
            if (key != 0 && key % PRINT_EACH == 0) {
               System.out.printf("\rInserted %d", key);
            }
         });
      }

      executorService.shutdown();
      executorService.awaitTermination(3, TimeUnit.MINUTES);
      if (!DISABLE_INDEX_WHEN_INSERTING) {
         waitForIndexSize(OBJECT_COUNT);
      }
      System.out.println();
   }

   /**
    * Waits until the index reaches a certain size. Useful for async backend
    */
   private void waitForIndexSize(final int expected) {
      eventually(() -> {
         long idxCount = countIndex();
         System.out.printf("\rWaiting for indexing completion (%d): %d indexed so far", expected, +idxCount);
         return idxCount == expected;
      });
      System.out.println("\nIndexing done.");
   }

   public static void main(String[] args) throws Throwable {
      AsyncMassIndexPerfTest test = new AsyncMassIndexPerfTest();
      TestResourceTracker.testThreadStarted(test.getTestName());
      test.createBeforeClass();
      test.createBeforeMethod();
      test.populate();
   }

   public void populate() throws Exception {
      StopTimer stopTimer = new StopTimer();
      writeData();
      stopTimer.stop();
      System.out.printf("\rData inserted in %d seconds.", stopTimer.getElapsedIn(TimeUnit.SECONDS));
      info();
      new Thread(new EventLoop(indexer)).start();
   }

   private void info() {
      System.out.println("\rr: Run MassIndexer\nc: Cancel MassIndexer\ni: Put new entry\ns: Current index size\np: Purge indexes\nf: flush\nh: This menu\nx: Exit");
   }

   private enum IndexManager {
      NRT("10000"),
      DIRECTORY("0");
      private final String cfg;

      IndexManager(String cfg) {
         this.cfg = cfg;
      }

      @Override
      public String toString() {
         return cfg;
      }
   }

   protected long countIndex() {
      Query<?> q = cache1.query("FROM " + Transaction.class.getName());
      return q.execute().count().value();
   }

   protected void clearIndex() {
      Search.getIndexer(cache1).remove();
   }

   class EventLoop implements Runnable {

      private Indexer indexer;
      private CompletionStage<Void> future;
      private AtomicInteger nexIndex = new AtomicInteger(OBJECT_COUNT);

      public EventLoop(Indexer indexer) {
         this.indexer = indexer;
      }

      void startMassIndexer() {
         System.out.println("Running MassIndexer");
         final StopTimer stopTimer = new StopTimer();
         future = indexer.run().toCompletableFuture();
         future.whenComplete((v, t) -> {
            stopTimer.stop();
            if (t != null) {
               System.out.println("Error executing massindexer");
               t.printStackTrace();
            }
            System.out.printf("\nMass indexer run in %d seconds", stopTimer.getElapsedIn(TimeUnit.SECONDS));
            System.out.println();
            waitForIndexSize(nexIndex.get());
            System.out.println("Mass Indexing complete.");
         });
      }

      @Override
      public void run() {
         Scanner scanner = new Scanner(System.in);
         while (!Thread.interrupted()) {
            String next = scanner.next();
            if ("c".equals(next)) {
               if (future == null) {
                  System.out.println("\rMassIndexer not started");
                  continue;
               } else {
                  // Mass Indexer doesn't provide cancellation currently
                  // https://issues.redhat.com/browse/ISPN-11735
//                  future.cancel(true);
                  System.out.println("Mass Indexer cancelled");
               }
            }
            if ("r".equals(next)) {
               startMassIndexer();
            }
            if ("f".equals(next)) {
               flushIndex();
               System.out.println("Index flushed.");
            }
            if ("s".equals(next)) {
               System.out.printf("Index size is %d\n", countIndex());
            }
            if ("p".equals(next)) {
               clearIndex();
               System.out.println("Index cleared.");
            }
            if ("i".equals(next)) {
               int nextIndex = nexIndex.incrementAndGet();
               cache2.put(nextIndex, new Transaction(nextIndex, "0" + nextIndex));
               System.out.println("New entry inserted");
            }
            if ("h".equals(next)) {
               info();
            }
            if ("x".equals(next)) {
               System.exit(0);
            }
         }
      }
   }

   private void flushIndex() {
      IndexUpdater indexUpdater = new IndexUpdater(ComponentRegistryUtils.getSearchMapping(cache1));
      indexUpdater.flush(Collections.singleton(Transaction.class));
   }

   static class StopTimer {
      private long start;
      private long elapsed;

      public StopTimer() {
         start = currentTime();
      }

      private long currentTime() {
         return System.currentTimeMillis();
      }

      public void reset() {
         start = currentTime();
      }

      public void stop() {
         elapsed = currentTime() - start;
      }

      public long getElapsedIn(TimeUnit unit) {
         return unit.convert(elapsed, TimeUnit.MILLISECONDS);
      }
   }
}
