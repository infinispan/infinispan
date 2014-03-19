package org.infinispan.eviction.impl;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.BaseEvictionFunctionalTest")
public abstract class BaseEvictionFunctionalTest extends SingleCacheManagerTest {
   
   private static final int CACHE_SIZE=128;
   
   protected BaseEvictionFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected abstract EvictionStrategy getEvictionStrategy();

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.eviction().maxEntries(CACHE_SIZE)
            .strategy(getEvictionStrategy()).expiration().wakeUpInterval(100L).locking()
            .useLockStriping(false) // to minimize chances of deadlock in the unit test
            .invocationBatching();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      cache = cm.getCache();
      cache.addListener(new EvictionListener());
      return cm;
   }

   public void testSimpleEvictionMaxEntries() throws Exception {
      for (int i = 0; i < CACHE_SIZE*2; i++) {
         cache.put("key-" + (i + 1), "value-" + (i + 1), 1, TimeUnit.MINUTES);
      }
      Thread.sleep(1000); // sleep long enough to allow the thread to wake-up
      assert CACHE_SIZE >= cache.size() : "cache size too big: " + cache.size();
   }
   
   public void testSimpleExpirationMaxIdle() throws Exception {

      for (int i = 0; i < CACHE_SIZE*2; i++) {
         cache.put("key-" + (i + 1), "value-" + (i + 1), 1, TimeUnit.MILLISECONDS);
      }
      Thread.sleep(1000); // sleep long enough to allow the thread to wake-up and purge all expired entries
      assert 0 == cache.size() : "cache size should be zero: " + cache.size();
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

      // wait for the cache size to drop to CACHE_SIZE, up to a specified amount of time.
      long giveUpTime = System.currentTimeMillis() + (1000 * 10); // 10 sec
      while (cache.getAdvancedCache().getDataContainer().size() > 1 && System.currentTimeMillis() < giveUpTime) {
         //System.out.println("Cache size is " + cache.size() + " and time diff is " + (giveUpTime - System.currentTimeMillis()));
         Thread.sleep(100);
      }

      assert cache.getAdvancedCache().getDataContainer().size() <= CACHE_SIZE : "Expected 1, was " + cache.size(); // this is what we expect the cache to be pruned to      
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
            
            //mix mortal and immortal entries 
            if (Math.random() < 0.5) {
               cache.put("key" + r.nextInt(), "value");
            } else {
               cache.put("key" + r.nextInt(), "value", 10, TimeUnit.SECONDS);
            }
         }
      }
   }
   
   @Listener
   public static class EvictionListener {
      
      @CacheEntriesEvicted
      public void nodeEvicted(CacheEntriesEvictedEvent e){
         assert e.isPre() || !e.isPre();
         Object key = e.getEntries().keySet().iterator().next();
         assert key != null;
         assert e.getCache() != null;
         assert e.getType() == Event.Type.CACHE_ENTRY_EVICTED;         
      }
   }
}
