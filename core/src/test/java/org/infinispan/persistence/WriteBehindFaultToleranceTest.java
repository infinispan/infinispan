package org.infinispan.persistence;


import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.async.AdvancedAsyncCacheWriter;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

@CleanupAfterMethod
@Test(testName = "persistence.WriteBehindFaultToleranceTest", groups = "functional")
public class WriteBehindFaultToleranceTest extends SingleCacheManagerTest {

   private static final int AVAILABILITY_INTERVAL  = 10;

   private Cache<Object, Object> createManagerAndGetCache(boolean failSilently, int queueSize) {
      GlobalConfigurationBuilder globalConfiguration = new GlobalConfigurationBuilder();
      ConfigurationBuilder config = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      config.persistence().availabilityInterval(AVAILABILITY_INTERVAL)
        .addStore(DummyInMemoryStoreConfigurationBuilder.class)
        .async().enable().modificationQueueSize(queueSize).failSilently(failSilently);
      cacheManager = TestCacheManagerFactory.createCacheManager(globalConfiguration, config);
      return cacheManager.getCache();
   }

   @Test
   public void testBlockingOnStoreAvailabilityChange() {
      Cache<Object, Object> cache = createManagerAndGetCache(false, 1);
      PollingPersistenceManager pm = new PollingPersistenceManager();
      PersistenceManager oldPersistenceManager = TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
      oldPersistenceManager.stop();
      AdvancedAsyncCacheWriter asyncWriter = TestingUtil.getFirstWriter(cache);
      DummyInMemoryStore store = (DummyInMemoryStore) TestingUtil.extractField(AdvancedAsyncCacheWriter.class, asyncWriter, "actual");
      store.setAvailable(true);
      cache.put(1, 1);
      eventually(() -> store.loadEntry(1) != null);
      assertEquals(1, store.size());

      store.setAvailable(false);
      assertFalse(store.isAvailable());

      int pollCount = pm.pollCount.get();
      // Wait until the stores availability has been checked before asserting that the pm is still available
      eventually(() -> pm.pollCount.get() > pollCount);
      // PM & AsyncWriter should still be available as the async modification queue is not full
      assertTrue(asyncWriter.isAvailable());
      assertFalse(TestingUtil.extractField(asyncWriter, "delegateAvailable"));
      assertTrue(pm.isAvailable());

      // Add entries >= modification queue size so that store is no longer available
      cache.putAll(intMap(0, 10));
      assertEquals(1, store.size());

      eventually(() -> !pm.isAvailable());
      // PM and writer should not be available as the async modification queue is now oversubscribed and the delegate is still unavailable
      assertFalse(asyncWriter.isAvailable());
      Exceptions.expectException(StoreUnavailableException.class, () -> cache.putAll(intMap(10, 20)));
      assertEquals(1, store.size());

      // Make the delegate available and ensure that the initially queued modifications exist in the store
      store.setAvailable(true);
      assertTrue(store.isAvailable());
      assertTrue(asyncWriter.isAvailable());
      eventually(pm::isAvailable);
      eventually(() -> 10 == store.size());
      // Ensure that only the initial map entries are stored and that the second putAll operation truly failed
      assertFalse(store.contains(10));
   }

   private Map<Integer, Integer> intMap(int start, int end) {
      return IntStream.range(start, end).boxed().collect(Collectors.toMap(Function.identity(), Function.identity()));
   }

   @Test
   public void testWritesFailSilentlyWhenConfigured() {
      Cache<Object, Object> cache = createManagerAndGetCache(true, 1);
      AdvancedAsyncCacheWriter asyncWriter = TestingUtil.getFirstWriter(cache);
      DummyInMemoryStore store = (DummyInMemoryStore) TestingUtil.extractField(AdvancedAsyncCacheWriter.class, asyncWriter, "actual");
      assertTrue(store.isAvailable());
      cache.put(1, 1);
      eventually(() -> store.loadEntry(1) != null);
      assertEquals(1, store.size());

      store.setAvailable(false);
      assertFalse(store.isAvailable());
      cache.put(1, 2); // Should fail on the store, but complete in-memory
      TestingUtil.sleepThread(1000); // Sleep to ensure async write is attempted
      store.setAvailable(true);
      MarshallableEntry entry = store.loadEntry(1);
      assertNotNull(entry);
      assertEquals(1, entry.getValue());
      assertEquals(2, cache.get(1));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      // Manager is created later
      return null;
   }

   @Override
   protected void setup() throws Exception {
      // Manager is created later
   }

   static class PollingPersistenceManager extends PersistenceManagerImpl {
      final AtomicInteger pollCount = new AtomicInteger();
      @Override
      protected void pollStoreAvailability() {
         super.pollStoreAvailability();
         pollCount.incrementAndGet();
      }
   }
}
