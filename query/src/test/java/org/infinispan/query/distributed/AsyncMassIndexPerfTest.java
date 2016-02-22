package org.infinispan.query.distributed;

import org.apache.lucene.search.MatchAllDocsQuery;
import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.FutureListener;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.context.Flag;
import org.infinispan.query.CacheQuery;
import org.infinispan.query.MassIndexer;
import org.infinispan.query.Search;
import org.infinispan.query.SearchManager;
import org.infinispan.query.impl.massindex.IndexUpdater;
import org.infinispan.test.MultipleCacheManagersTest;

import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Long running test for the async MassIndexer, specially regarding cancellation. Supposed to be run as a main class.
 *
 * @author gustavonalle
 * @since 7.1
 */
public class AsyncMassIndexPerfTest extends MultipleCacheManagersTest {

   /**
    * Number of entries to write
    */
   private static final int OBJECT_COUNT = 1000000;
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
   private static final CacheMode CACHE_MODE = CacheMode.DIST_SYNC;
   private static final IndexManager INDEX_MANAGER = IndexManager.INFINISPAN;
   private static final Provider DIRECTORY_PROVIDER = Provider.INFINISPAN;
   /**
    * Hibernate search backend used. Either sync or async (commit every 1s by default)
    */
   private static final WorkerMode WORKER_MODE = WorkerMode.sync;
   /**
    * For status report during insertion
    */
   private static final int PRINT_EACH = 10;

   private Cache<Integer, Transaction> cache1, cache2;
   private MassIndexer massIndexer;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cacheCfg = getDefaultClusteredCacheConfig(CACHE_MODE, TX_ENABLED);
      cacheCfg.clustering().sync().replTimeout(120000)
            .indexing().index(Index.LOCAL)
            .addIndexedEntity(Transaction.class)
            .addProperty("default.directory_provider", DIRECTORY_PROVIDER.toString())
            .addProperty("default.indexmanager", INDEX_MANAGER.toString())
            .addProperty("default.indexwriter.merge_factor", MERGE_FACTOR)
            .addProperty("hibernate.search.default.worker.execution", WORKER_MODE.toString())
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      ;
      List<Cache<Integer, Transaction>> caches = createClusteredCaches(2, cacheCfg);
      cache1 = caches.get(0);
      cache2 = caches.get(1);
      massIndexer = Search.getSearchManager(cache1).getMassIndexer();
   }

   private void writeData() throws InterruptedException {
      ExecutorService executorService = Executors.newFixedThreadPool(WRITING_THREADS, getTestThreadFactory("Worker"));
      final AtomicInteger counter = new AtomicInteger(0);
      for (int i = 0; i < OBJECT_COUNT; i++) {
         executorService.submit(new Runnable() {
            @Override
            public void run() {
               int key = counter.incrementAndGet();
               Cache insertCache;
               if (DISABLE_INDEX_WHEN_INSERTING) {
                  insertCache = cache1.getAdvancedCache().withFlags(Flag.SKIP_INDEXING);
               } else {
                  insertCache = cache1;
               }
               insertCache.put(key, new Transaction(key * 100, "0eab" + key));
               if (key != 0 && key % PRINT_EACH == 0) {
                  System.out.printf("\rInserted %d", key);
               }
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
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            int idxCount = countIndex();
            System.out.printf("\rWaiting for indexing completion: %d indexed so far", +idxCount);
            return idxCount == expected;
         }
      });
      System.out.println("\nIndexing done.");
   }


   public static void main(String[] args) throws Throwable {
      AsyncMassIndexPerfTest test = new AsyncMassIndexPerfTest();
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
      new Thread(new EventLoop(massIndexer)).start();
   }

   private void info() {
      System.out.println("\rTo run MassIndexer, press 'r'. To cancel press 'c'. To put new entry, press 'i'. For index size, type 's', 'p' to purge it, and 'f' to flush");
   }

   private enum Provider {
      RAM("ram"),
      FILESYSTEM("filesystem"),
      INFINISPAN("infinispan");
      private final String cfg;

      Provider(String cfg) {
         this.cfg = cfg;
      }

      @Override
      public String toString() {
         return cfg;
      }
   }

   private enum WorkerMode {
      async,
      sync
   }

   private enum IndexManager {
      NRT("near-real-time"),
      INFINISPAN("org.infinispan.query.indexmanager.InfinispanIndexManager"),
      DIRECTORY("directory-based");
      private final String cfg;

      IndexManager(String cfg) {
         this.cfg = cfg;
      }

      @Override
      public String toString() {
         return cfg;
      }
   }

   protected int countIndex() {
      SearchManager searchManager = Search.getSearchManager(cache1);
      CacheQuery q = searchManager.getQuery(new MatchAllDocsQuery(), Transaction.class);
      return q.getResultSize();
   }

   protected void clearIndex() {
      SearchManager searchManager = Search.getSearchManager(cache1);
      searchManager.purge(Transaction.class);
   }

   class EventLoop implements Runnable {

      private MassIndexer massIndexer;
      private NotifyingFuture<Void> future;
      private AtomicInteger nexIndex = new AtomicInteger(OBJECT_COUNT);

      public EventLoop(MassIndexer massIndexer) {
         this.massIndexer = massIndexer;
      }

      void startMassIndexer() {
         System.out.println("Running MassIndexer");
         final StopTimer stopTimer = new StopTimer();
         future = massIndexer.startAsync();
         future.attachListener(new FutureListener<Void>() {
            @Override
            public void futureDone(Future<Void> future) {
               stopTimer.stop();
               try {
                  future.get();
                  System.out.printf("\nMass indexer run in %d seconds", stopTimer.getElapsedIn(TimeUnit.SECONDS));
                  System.out.println();
                  waitForIndexSize(nexIndex.get());
                  System.out.println("Mass Indexing complete.");
               } catch (Exception e) {
                  System.out.println("Error executing massindexer");
                  e.printStackTrace();
               }
            }
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
                  future.cancel(true);
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
            info();
         }
      }
   }

   private void flushIndex() {
      new IndexUpdater(cache1).flush(Transaction.class);
   }

   class StopTimer {
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
