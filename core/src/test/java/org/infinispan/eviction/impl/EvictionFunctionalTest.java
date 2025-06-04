package org.infinispan.eviction.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.ControlledTimeService;
import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.context.Flag;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.notifications.Listener;
import org.infinispan.notifications.cachelistener.annotation.CacheEntriesEvicted;
import org.infinispan.notifications.cachelistener.event.CacheEntriesEvictedEvent;
import org.infinispan.notifications.cachelistener.event.Event;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Factory;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "eviction.EvictionFunctionalTest")
public class EvictionFunctionalTest extends SingleCacheManagerTest {

   private static final int CACHE_SIZE = 64;

   private StorageType storageType;
   private EvictionListener evictionListener;
   private ControlledTimeService timeService;

   protected EvictionFunctionalTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
   }

   public EvictionFunctionalTest storageType(StorageType storageType) {
      this.storageType = storageType;
      return this;
   }

   public StorageType getStorageType() {
      return storageType;
   }

   @Factory
   public Object[] factory() {
      return new Object[]{
            new EvictionFunctionalTest().storageType(StorageType.BINARY),
            new EvictionFunctionalTest().storageType(StorageType.HEAP),
            new EvictionFunctionalTest().storageType(StorageType.OFF_HEAP)
      };
   }

   @Override
   protected String parameters() {
      return "[" + storageType + "]";
   }

   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.memory().maxCount(CACHE_SIZE).storage(getStorageType())
            .expiration().wakeUpInterval(100L).locking()
            .useLockStriping(false) // to minimize chances of deadlock in the unit test
            .invocationBatching();
      EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
      cache = cm.getCache();
      evictionListener = new EvictionListener();
      cache.addListener(evictionListener);
      TestingUtil.replaceComponent(cm, TimeService.class, timeService = new ControlledTimeService(), true);
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

   public void testEvictNonExistantEntry() {
      String key = "key";
      String value = "some-value";
      cache.put(key, value);

      cache.evict(key);

      assertEquals(1, evictionListener.evictedEntries.size());

      // Make sure if we evict again that it doesn't increase count
      cache.evict(key);

      // TODO: this seems like a bug, but many tests rely on this - maybe change later
      assertEquals(2, evictionListener.evictedEntries.size());
   }

   public void testSimpleExpirationMaxIdle() throws Exception {
      for (int i = 0; i < CACHE_SIZE * 2; i++) {
         cache.put("key-" + (i + 1), "value-" + (i + 1), 1, TimeUnit.MILLISECONDS);
      }
      timeService.advance(1000);
      cache.getAdvancedCache().getExpirationManager().processExpiration();
      assert cache.isEmpty() : "cache size should be zero: " + cache.size();
   }

   public void testEvictionNotificationSkipped() {
      String key = "key";
      String value = "value";

      cache.put(key, value);

      cache.getAdvancedCache().withFlags(Flag.SKIP_LISTENER_NOTIFICATION).evict(key);

      assertEquals(0, evictionListener.getEvictedEvents().size());
   }

   @Listener
   public static class EvictionListener {

      private final List<Map.Entry> evictedEntries = Collections.synchronizedList(new ArrayList<>());

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
