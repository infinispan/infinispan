package org.infinispan.eviction.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
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

import org.infinispan.commons.test.Exceptions;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.Flag;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.spi.MarshallableEntry;
import org.infinispan.persistence.support.WaitNonBlockingStore;
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
      eventuallyEquals(2L, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   @Override
   public void testEvictionDuringRemove() throws InterruptedException, ExecutionException, TimeoutException {
      super.testEvictionDuringRemove();
      eventuallyEquals(0L, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
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
      eventuallyEquals(3L, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   public void testEvictionDuringWriteWithConcurrentRead() throws TimeoutException, InterruptedException, ExecutionException {
      String key = "evicted-key";

      // We use this checkpoint to stop write orderer for "evicted-key" from being released
      // Not releasing the orderer blocks prevents another passivation or activation of the same key
      CheckPoint operationCheckPoint = new CheckPoint();

      // Blocks just before releasing the orderer for evicted-key
      // Note: Cannot use eq(WRITE) because eviction uses READ
      Mocks.blockingMock(operationCheckPoint, DataOperationOrderer.class, cache, AdditionalAnswers::delegatesTo,
            (stub, m) -> stub.when(m).completeOperation(eq(key), any(), any()));

      // Put the key which will wait on releasing the orderer at the end
      Future<Object> operationFuture = fork(() -> cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD).put(key, "value"));
      // Confirm the entry has been inserted in the data container so we can evict
      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, SECONDS);

      // The eviction blocks, but it does not prevent the put operation from finishing
      cache.put("other-key", "other-value");
      assertNull(operationCheckPoint.peek(50, TimeUnit.MILLISECONDS, Mocks.BEFORE_INVOCATION));

      // Let put(evicted-key) release the orderer and finish the operation
      operationCheckPoint.trigger(Mocks.BEFORE_RELEASE);
      operationCheckPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, SECONDS);
      operationCheckPoint.trigger(Mocks.AFTER_RELEASE);
      operationFuture.get(10, SECONDS);

      // put(other-key)'s eviction is still holding evicted-key's orderer
      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, SECONDS);

      // Start get(evicted-key), it cannot complete yet
      Future<Object> getFuture = fork(() -> cache.get(key));
      Exceptions.expectException(TimeoutException.class, () -> getFuture.get(50, TimeUnit.MILLISECONDS));

      // Let the put(other-key) eviction release the orderer, it will be acquired by get(evicted-key)
      operationCheckPoint.trigger(Mocks.BEFORE_RELEASE);
      operationCheckPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, SECONDS);
      operationCheckPoint.trigger(Mocks.AFTER_RELEASE);

      // get(evicted-key) can now finish, even though it cannot release evicted-key's orderer
      assertNotNull(getFuture.get(10, SECONDS));

      // Let get(evicted-key) release the orderer
      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, SECONDS);
      operationCheckPoint.trigger(Mocks.BEFORE_RELEASE);
      operationCheckPoint.awaitStrict(Mocks.AFTER_INVOCATION, 10, SECONDS);
      operationCheckPoint.trigger(Mocks.AFTER_RELEASE);

      // #1 evicted-key evicted by other-key from write
      // #2 other-key evicted by evicted-key from the get
      eventuallyEquals(2L, () -> TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
      // #1 evicted key activated from the get
      eventuallyEquals(1L, () -> TestingUtil.extractComponent(cache, ActivationManager.class).getActivationCount());
      eventuallyEquals(0L, () -> TestingUtil.extractComponent(cache, ActivationManager.class).getPendingActivationCount());
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

      operationCheckPoint.awaitStrict(Mocks.BEFORE_INVOCATION, 10, SECONDS);

      // Now restore the original orderer so our put can properly retrieve it
      TestingUtil.replaceComponent(cache, DataOperationOrderer.class, original, true);

      String newValue = "value-2";
      Future<Object> evictedKeyPutFuture = fork(() -> cache.put(key, newValue));

      // Should be blocked waiting on Caffeine lock - but has the orderer
      Exceptions.expectException(TimeoutException.class, () -> evictedKeyPutFuture.get(50, TimeUnit.MILLISECONDS));

      // Let the eviction finish, which will let the put happen
      operationCheckPoint.trigger(Mocks.BEFORE_RELEASE);

      putFuture.get(10, SECONDS);

      assertEquals(initialValue, evictedKeyPutFuture.get(10, SECONDS));

      assertInMemory(key, newValue);

      PassivationPersistenceManager ppm = (PassivationPersistenceManager) TestingUtil.extractComponent(cache, PersistenceManager.class);
      eventuallyEquals(0, ppm::pendingPassivations);

      assertEquals(1L, TestingUtil.extractComponent(cache, PassivationManager.class).getPassivations());
   }

   @Override
   protected void initializeKeyAndCheckData(Object key, Object value) {
      assertTrue("A cache store should be configured!", cache.getCacheConfiguration().persistence().usingStores());
      cache.put(key, value);
      DataContainer<?, ?> container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry<?, ?> entry = container.peek(key);
      WaitNonBlockingStore<?, ?> loader = TestingUtil.getFirstStoreWait(cache);
      assertNotNull("Key " + key + " does not exist in data container.", entry);
      assertEquals("Wrong value for key " + key + " in data container.", value, entry.getValue());
      MarshallableEntry<?, ?> entryLoaded = loader.loadEntry(key);
      assertNull("Key " + key + " exists in cache loader.", entryLoaded);
   }

   @Override
   protected void assertInMemory(Object key, Object value) {
      DataContainer<?, ?> container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry<?, ?> entry = container.get(key);
      WaitNonBlockingStore<?, ?> loader = TestingUtil.getFirstStoreWait(cache);
      assertNotNull("Key " + key + " does not exist in data container", entry);
      assertEquals("Wrong value for key " + key + " in data container", value, entry.getValue());
      eventually(() -> loader.loadEntry(key) == null);
   }

   @Override
   protected void assertNotInMemory(Object key, Object value) {
      DataContainer<?, ?> container = cache.getAdvancedCache().getDataContainer();
      InternalCacheEntry<?, ?> entry = container.get(key);
      WaitNonBlockingStore<Object, Object> loader = TestingUtil.getFirstStoreWait(cache);
      assertNull("Key " + key + " exists in data container", entry);
      MarshallableEntry<Object, Object> entryLoaded = loader.loadEntry(key);
      assertNotNull("Key " + key + " does not exist in cache loader", entryLoaded);
      assertEquals("Wrong value for key " + key + " in cache loader", value, entryLoaded.getValue());
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      // Enable stats so we can count passivations
      builder.statistics().enable();
      builder.persistence().passivation(true)
            // This test doesn't work with DummyInMemoryStore as the test will block the invoking thread with the orderer
            // We therefore use a store that requires offloading the write to another thread which prevents this
            .addSingleFileStore();
   }
}
