package org.infinispan.eviction.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.eviction.EvictionStrategy;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

@Test(groups = "functional", testName = "eviction.BaseEvictionFunctionalTest")
public abstract class BaseEvictionFunctionalTest extends SingleCacheManagerTest {

   private static final int CACHE_SIZE = 64;

   private EvictionListener evictionListener;

   protected BaseEvictionFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   protected abstract StorageType getStorageType();

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.memory().size(CACHE_SIZE).storageType(getStorageType())
            .expiration().wakeUpInterval(100L).locking()
            .useLockStriping(false) // to minimize chances of deadlock in the unit test
            .invocationBatching();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      cache = cm.getCache();
      evictionListener = new EvictionListener();
      cache.addListener(evictionListener);
      return cm;
   }

   public void testSimpleEvictionMaxEntries() throws Exception {
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         cache.put("key-" + (i + 1), "value-" + (i + 1));
      }
      assertEquals("cache size too big: " + cache.size(), CACHE_SIZE, cache.size());
      assertEquals("eviction events count should be same with case size: " + evictionListener.getEvictedEvents(),
            CACHE_SIZE, evictionListener.getEvictedEvents().size());

      for (int i = 0; i < CACHE_SIZE; i++) {
         cache.put("key-" + (i + 1), "value-" + (i + 1));
      }
      assertEquals(CACHE_SIZE, cache.size());
      // We don't know for sure how many will be evicted due to randomness, but we know they MUST evict
      // at least a size worth since we are writing more than double
      assertTrue(evictionListener.evictedEntries.size() > CACHE_SIZE);
   }

   public void testSimpleExpirationMaxIdle() throws Exception {

      for (int i = 0; i < CACHE_SIZE * 2; i++) {
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
      long giveUpTime = System.currentTimeMillis() + (1000 * Writer.LIFESPAN);
      while (cache.getAdvancedCache().getDataContainer().size() > 1 && System.currentTimeMillis() < giveUpTime) {
         //System.out.println("Cache size is " + cache.size() + " and time diff is " + (giveUpTime - System.currentTimeMillis()));
         Thread.sleep(100);
      }

      assertTrue(String.format("Cache was expected to be pruned to %d, but was %d", CACHE_SIZE, cache.size()),
            cache.getAdvancedCache().getDataContainer().size() <= CACHE_SIZE);
   }

   private class Writer extends Thread {
      public static final int LIFESPAN = 10;
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
               cache.put("key" + r.nextInt(), "value", LIFESPAN, TimeUnit.SECONDS);
            }
         }
      }
   }

   @Listener
   public static class EvictionListener {

      private List<Map.Entry> evictedEntries = Collections.synchronizedList(new ArrayList<>());

      @CacheEntriesEvicted
      public void nodeEvicted(CacheEntriesEvictedEvent e) {
         assert e.isPre() || !e.isPre();
         Object key = e.getEntries().keySet().iterator().next();
         assert key != null;
         assert e.getCache() != null;
         assert e.getType() == Event.Type.CACHE_ENTRY_EVICTED;
         e.getEntries().entrySet().stream().forEach(entry -> evictedEntries.add((Map.Entry) entry));
      }

      public List<Map.Entry> getEvictedEvents() {
         return evictedEntries;
      }
   }
}
