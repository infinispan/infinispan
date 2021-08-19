package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.spi.StoreUnavailableException;
import org.infinispan.persistence.support.DelegatingNonBlockingStore;
import org.infinispan.persistence.support.WaitNonBlockingStore;
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

   public void testBlockingOnStoreAvailabilityChange() throws InterruptedException, ExecutionException, TimeoutException {
      Cache<Object, Object> cache = createManagerAndGetCache(false, 1);
      PollingPersistenceManager pm = new PollingPersistenceManager();
      PersistenceManager oldPersistenceManager = TestingUtil.replaceComponent(cache, PersistenceManager.class, pm, true);
      oldPersistenceManager.stop();
      WaitNonBlockingStore<?, ?> asyncStore = TestingUtil.getStore(cache, 0, false);
      DummyInMemoryStore dims = TestingUtil.getStore(cache, 0, true);
      dims.setAvailable(true);
      cache.put(1, 1);
      eventually(() -> dims.loadEntry(1) != null);
      assertEquals(1, dims.size());

      dims.setAvailable(false);
      assertFalse(dims.checkAvailable());

      int pollCount = pm.pollCount.get();
      // Wait until the store's availability has been checked before asserting that the pm is still available
      eventually(() -> pm.pollCount.get() > pollCount);
      // PM & AsyncWriter should still be available as the async modification queue is not full
      assertTrue(asyncStore.checkAvailable());
      // The asyncStore is a WaitNonBlockingStore delegate wrapping the actual async store
      assertNotNull(TestingUtil.extractField(((DelegatingNonBlockingStore) asyncStore).delegate(), "delegateAvailableFuture"));
      assertTrue(pm.isAvailable());

      Future<Void> f = fork(() -> {
         // Add entries >= modification queue size so that store is no longer available
         // The async store creates 2 batches:
         // 1. modification 1 returns immediately, but stays in the queue until DIMS becomes available again
         // 2. modifications 2-5 block in the async store because the modification queue is full
         // The async store waits for the DIMS to become available before completing the 1st batch
         // After PersistenceManagerImpl sees the store is unavailable, it becomes unavailable itself
         // and later writes never reach the async store (until PMI becomes available again).
         cache.putAll(intMap(0, 5));
      });
      assertEquals(1, dims.size());

      eventually(() -> !pm.isAvailable());
      // PM and writer should not be available as the async modification queue is now oversubscribed and the delegate is still unavailable
      Exceptions.expectException(StoreUnavailableException.class, () -> cache.putAll(intMap(10, 20)));
      assertEquals(1, dims.size());

      // Make the delegate available and ensure that the initially queued modifications exist in the store
      dims.setAvailable(true);
      assertTrue(asyncStore.checkAvailable());
      eventually(pm::isAvailable);
      f.get(10, TimeUnit.SECONDS);

      // Now that PMI is available again, a new write will succeed
      cache.putAll(intMap(5, 10));

      // Ensure that the putAll(10..20) operation truly failed
      eventuallyEquals(IntSets.immutableRangeSet(10), dims::keySet);
   }

   private Map<Integer, Integer> intMap(int start, int end) {
      return IntStream.range(start, end).boxed().collect(Collectors.toMap(Function.identity(), Function.identity()));
   }

   public void testWritesFailSilentlyWhenConfigured() {
      Cache<Object, Object> cache = createManagerAndGetCache(true, 1);
      DummyInMemoryStore dims = TestingUtil.getStore(cache, 0, true);
      assertTrue(dims.checkAvailable());
      cache.put(1, 1);
      eventually(() -> dims.loadEntry(1) != null);
      assertEquals(1, dims.size());

      dims.setAvailable(false);
      assertFalse(dims.checkAvailable());
      cache.put(1, 2); // Should fail on the store, but complete in-memory
      TestingUtil.sleepThread(1000); // Sleep to ensure async write is attempted
      dims.setAvailable(true);
      MarshallableEntry<?, ?> entry = dims.loadEntry(1);
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
      protected CompletionStage<Void> pollStoreAvailability() {
         pollCount.incrementAndGet();
         return super.pollStoreAvailability();
      }
   }
}
