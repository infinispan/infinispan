package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.commons.equivalence.ByteArrayEquivalence;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.fail;

@Test(groups = "stress", testName = "client.hotrod.ReplaceWithVersionConcurrencyTest")
public class ReplaceWithVersionConcurrencyTest extends MultiHotRodServersTest {

   static final AtomicInteger globalCounter = new AtomicInteger();
   static final String KEY = "A";
   static final int NUM_THREADS = 20;
   static final int OPS_PER_THREAD = 200;
   static final int TIMEOUT_MINUTES = 5;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
//            .transaction()
//            .lockingMode(LockingMode.PESSIMISTIC)
//            .transactionMode(TransactionMode.TRANSACTIONAL)
      ;
      createHotRodServers(2, builder);
   }

   public void testKeepingCounterWithReplaceWithVersion() throws Exception {
      RemoteCache<String, Integer> cache = client(0).getCache();
      assertNull(cache.get(KEY));
      long timeSpent = System.currentTimeMillis();
      List<Future<Integer>> results = new ArrayList<Future<Integer>>(NUM_THREADS);
      ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);
      for (int i = 0; i < NUM_THREADS; i++) {
         CounterUpdater app = new CounterUpdater(cache, KEY, OPS_PER_THREAD);
         Future<Integer> result = executor.submit(app);
         results.add(result);
      }
      executor.shutdown();
      executor.awaitTermination(TIMEOUT_MINUTES, TimeUnit.MINUTES);
      timeSpent = System.currentTimeMillis() - timeSpent;

      int actual = cache.get(KEY); // server-side value
      int expected = 0; // client-side value
      for (Future<Integer> f : results)
         expected += f.get();

      log.info("Time spent: " + timeSpent / 1000.0 + " secs.");
      assertEquals(expected, actual);
   }

   static class CounterUpdater implements Callable<Integer> {
      static final Log log = LogFactory.getLog(CounterUpdater.class);
      final RemoteCache<String, Integer> cache;
      final String key;
      final int limit;

      CounterUpdater(RemoteCache<String, Integer> cache, String key, int limit) {
         this.cache = cache;
         this.key = key;
         this.limit = limit;
      }

      @Override
      public Integer call() throws Exception {
         int counter = 0;
         log.info("Start to count.");
         int i = 0;
         while (i < limit) {
            incrementCounter();
            counter++;
            i++;
         }
         log.info("Counted " + counter);
         return counter;
      }

      private void incrementCounter() {
         while (true) {
            VersionedValue<Integer> versioned = cache.getVersioned(key);
            if (versioned == null) {
               if (cache.withFlags(Flag.FORCE_RETURN_VALUE).putIfAbsent(key, 1) == null) {
                  log.info("count=" + globalCounter.getAndIncrement() + ",prev=0,new=1 (first-put)");
                  return;
               }
            } else {
               int val = versioned.getValue() + 1;
               long version = versioned.getVersion();
               if (cache.replaceWithVersion(key, val, version)) {
                  int count = globalCounter.getAndIncrement();
                  log.info("count=" + count +",prev=" + versioned.getValue() + ",new=" + val + ",prev-version=" + version);
                  // Optimistically updated, no more retries.
                  return;
               }
            }
         }
      }


   }

}
