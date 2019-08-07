package org.infinispan.eviction.impl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.eviction.PassivationManager;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.test.Exceptions;
import org.infinispan.test.Mocks;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.concurrent.DataOperationOrderer;
import org.mockito.AdditionalAnswers;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests size-based eviction with concurrent read and/or write operation with passivation enabled.
 *
 * @author Pedro Ruivo
 * @since 6.0
 */
@Test(groups = "functional", testName = "eviction.EvictionWithPassivationAndConcurrentOperationsTest")
public class EvictionWithPassivationAndConcurrentOperationsTest extends EvictionWithConcurrentOperationsTest {

   @Override
   public void testEvictionDuringWrite() throws InterruptedException, ExecutionException, TimeoutException {
      super.testEvictionDuringWrite();
      // #1 evicted-key evicted from write of other-key
      // #2 other-key is evicted when evicted-key is retrieved as last step
      eventuallyEquals(2l, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   @Override
   public void testEvictionDuringRemove() throws InterruptedException, ExecutionException, TimeoutException {
      super.testEvictionDuringRemove();
      eventuallyEquals(0l, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   // Cache store loads entry but before releasing orderer the entry is evicted. In this case the entry
   // should be passivated to ensure data is still around
   public void testEvictionDuringLoad() throws InterruptedException, ExecutionException, TimeoutException {
      String key = "evicted-key";
      cache.put(key, "loaded");
      // Ensures the key is in the store - note this is one passivation
      cache.evict(key);
      testEvictionDuring(key, () -> cache.get(key), AssertJUnit::assertNotNull, AssertJUnit::assertNotNull, true);

      // #1 evict above
      // #2 evicted-key evicted from write of other-key
      // #3 other-key is evicted when evicted-key is retrieved as last step
      eventuallyEquals(3l, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   public void testEvictionDuringWriteWithConcurrentRead() throws TimeoutException, InterruptedException, ExecutionException {
      String key = "evicted-key";

      // We use this checkpoint to stop write orderer from being released - but evicting the key
      CheckPoint operationCheckPoint = new CheckPoint();
      // Only trigger once for the write operation
      operationCheckPoint.trigger(Mocks.BEFORE_RELEASE);

      // Blocks just before releasing the orderer
      Mocks.blockingMock(operationCheckPoint, DataOperationOrderer.class, cache, AdditionalAnswers::delegatesTo,
            (stub, m) -> stub.when(m).completeOperation(eq(key), any(), any()));

      // Put the key which will wait on releasing the orderer at the end
      Future<Object> operationFuture = fork(() -> cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put(key, "value"));
      // Confirm the put has completed so we can evict
      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Note that the eviction is non blocking, so this should return just fine
      cache.put("other-key", "other-value");

      Future<Object> getFuture = fork(() -> cache.get(key));
      // Get shouldn't complete yet as eviction has hold of orderer
      Exceptions.expectException(TimeoutException.class, () -> getFuture.get(50, TimeUnit.MILLISECONDS));

      // Let the operation complete, which in turn lets the eviction return, which lets the get return
      // (gets with passivation that hit store have to acquire orderer)
      operationCheckPoint.triggerAll();
      operationFuture.get(10, TimeUnit.SECONDS);

      assertNotNull(getFuture.get(10, TimeUnit.SECONDS));

      // #1 evicted-key evicted by other-key from write
      // #2 other-key evicted by evicted-key from the get
      eventuallyEquals(2l, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   // This test differs from testEvictionDuringWrite in that it simulates an eviction and acquires the
   // caffeine lock, but is unable to acquire the orderer as it is already taken by a write operation. In this case
   // the eviction has removed the entry and the write puts it back - however the passivation should be skipped
   public void testWriteDuringEviction() throws TimeoutException, InterruptedException, ExecutionException {
      String key = "evicted-key";
      String initialValue = "value";
      cache.put(key, initialValue);

      // We use this checkpoint to stop eviction from acquiring the orderer
      CheckPoint operationCheckPoint = new CheckPoint();
      operationCheckPoint.triggerForever(Mocks.AFTER_RELEASE);

      // Blocks eviction from acquiring orderer - but has entry lock
      DataOperationOrderer original = Mocks.blockingMock(operationCheckPoint, DataOperationOrderer.class, cache, AdditionalAnswers::delegatesTo,
            (stub, m) -> stub.when(m).orderOn(eq(key), any()));

      // This will be stuck evicting the key until it can get the orderer
      Future<Object> putFuture = fork(() -> cache.put("other-key", "other-value"));

      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS);

      // Now restore the original orderer so our put can properly retrieve it
      TestingUtil.replaceComponent(cache, DataOperationOrderer.class, original, true);

      String newValue = "value-2";
      Future<Object> evictedKeyPutFuture = fork(() -> cache.put(key, newValue));

      // Should be blocked waiting on Caffeine lock - but has the orderer
      Exceptions.expectException(TimeoutException.class, () -> evictedKeyPutFuture.get(50, TimeUnit.MILLISECONDS));

      // Let the eviction finish, which will let the put happen
      operationCheckPoint.triggerAll();

      putFuture.get(10, TimeUnit.SECONDS);

      assertEquals(initialValue, evictedKeyPutFuture.get(10, TimeUnit.SECONDS));

      assertInMemory(key, newValue);

      PassivationPersistenceManager ppm = (PassivationPersistenceManager) TestingUtil.extractComponent(cache, PersistenceManager.class);
      eventuallyEquals(0, ppm::pendingPassivations);

      assertEquals(1l, TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void initializeKeyAndCheckData(Object key, Object value) {
      assertTrue("A cache store should be configured!", cache.getCacheConfiguration().persistence().usingStores());
      cache.put(key, value);
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNotNull("Key " + key + " does not exist in data container.", entry);
      assertEquals("Wrong value for key " + key + " in data container.", value, entry.getValue());
      MarshallableEntry<Object, Object> entryLoaded = loader.loadEntry(key);
      assertNull("Key " + key + " exists in cache loader.", entryLoaded);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertInMemory(Object key, Object value) {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNotNull("Key " + key + " does not exist in data container", entry);
      assertEquals("Wrong value for key " + key + " in data container", value, entry.getValue());
      eventually(() -> loader.loadEntry(key) == null);
   }

   @SuppressWarnings("unchecked")
   @Override
   protected void assertNotInMemory(Object key, Object value) {
      DataContainer container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry entry = container.get(key);
      CacheLoader<Object, Object> loader = TestingUtil.getFirstLoader(cache);
      assertNull("Key " + key + " exists in data container", entry);
      MarshallableEntry<Object, Object> entryLoaded = loader.loadEntry(key);
      assertNotNull("Key " + key + " does not exist in cache loader", entryLoaded);
      assertEquals("Wrong value for key " + key + " in cache loader", value, entryLoaded.getValue());
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      // Enable stats so we can count passivations
      builder.jmxStatistics().enable();
      builder.persistence().passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(storeName + storeNamePrefix.getAndIncrement());
   }
}
