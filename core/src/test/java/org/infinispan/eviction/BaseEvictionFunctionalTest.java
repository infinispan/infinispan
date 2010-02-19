package org.infinispan.eviction;

import org.infinispan.Cache;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

@Test(groups = "functional", testName = "eviction.BaseEvictionFunctionalTest")
public abstract class BaseEvictionFunctionalTest extends SingleCacheManagerTest {
   
   protected BaseEvictionFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected abstract EvictionStrategy getEvictionStrategy();

   protected CacheManager createCacheManager() throws Exception {
      Configuration cfg = new Configuration();
      cfg.setEvictionStrategy(getEvictionStrategy());
      cfg.setEvictionWakeUpInterval(100);
      cfg.setEvictionMaxEntries(1); // 1 max entries
      cfg.setUseLockStriping(false); // to minimise chances of deadlock in the unit test
      CacheManager cm = TestCacheManagerFactory.createCacheManager(cfg);
      cache = cm.getCache();
      return cm;
   }

   public void testMultiThreaded() throws InterruptedException {
      int NUM_THREADS = 20;
      Writer[] w = new Writer[NUM_THREADS];
      CountDownLatch startLatch = new CountDownLatch(1);

      for (int i = 0; i < NUM_THREADS; i++) w[i] = new Writer(i, startLatch);
      for (Writer writer : w) writer.start();

      startLatch.countDown();

      Thread.sleep(250);

      // now stop writers
      for (Writer writer : w) writer.running = false;
      for (Writer writer : w) writer.join();

      // wait for the cache size to drop to 1, up to a specified amount of time.
      long giveUpTime = System.currentTimeMillis() + (1000 * 60); // 1 min?
      while (cache.getAdvancedCache().getDataContainer().size() > 1 && System.currentTimeMillis() < giveUpTime) {
//         System.out.println("Cache size is " + cache.size() + " and time diff is " + (giveUpTime - System.currentTimeMillis()));
         Thread.sleep(100);
      }

      assert cache.getAdvancedCache().getDataContainer().size() == 1 : "Expected 1, was " + cache.size(); // this is what we expect the cache to be pruned to      
   }

   private class Writer extends Thread {
      CountDownLatch startLatch;
      volatile boolean running = true;
      Random r = new Random();

      public Writer(int n, CountDownLatch startLatch) {
         super("Writer-" + n);
         this.startLatch = startLatch;
         setDaemon(true);
      }

      @Override
      public void run() {
         try {
            startLatch.await();
         } catch (InterruptedException e) {
            // ignore
         }

         while (running) {
            try {
               sleep(r.nextInt(5) * 10);
            } catch (InterruptedException e) {
               // ignore
            }
            cache.put("key" + r.nextInt(), "value");
         }
      }
   }
}
