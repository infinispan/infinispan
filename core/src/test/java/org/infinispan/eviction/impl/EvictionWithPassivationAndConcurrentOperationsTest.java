package org.infinispan.eviction.impl;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotSame;
import static org.testng.AssertJUnit.assertSame;
import static org.testng.AssertJUnit.assertTrue;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PassivationPersistenceManager;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.util.concurrent.DataOperationOrderer;
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
   {
      passivation = true;
   }

   @Override
   public void testEvictionDuringWrite() throws InterruptedException, ExecutionException, TimeoutException {
      super.testEvictionDuringWrite();
      // #1 evicted-key evicted from write of other-key
      // #2 other-key is evicted when evicted-key is retrieved as last step
      eventuallyEquals(2L, () -> extractComponent(cache, PassivationManager.class).getPassivations());
   }

   @Override
   public void testEvictionDuringRemove() throws InterruptedException, ExecutionException, TimeoutException {
      super.testEvictionDuringRemove();
      eventuallyEquals(0L, () -> extractComponent(cache, PassivationManager.class).getPassivations());
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
      eventuallyEquals(3L, () -> extractComponent(cache, PassivationManager.class).getPassivations());
   }

   public void testEvictionDuringWriteWithConcurrentRead() throws TimeoutException, InterruptedException, ExecutionException {
      String key = "evicted-key";
      String value = "value";

      // Simulate a write orderer operation to acquire the write orderer for evicted-key
      // Holding the orderer blocks prevents another passivation or activation of the same key
      DataOperationOrderer orderer = extractComponent(cache, DataOperationOrderer.class);
      CompletableFuture<DataOperationOrderer.Operation> delayFuture1 = acquireOrderer(orderer, key, null);
      log.tracef("delayFuture1=%s", delayFuture1.toString());

      // Put the key which will wait on releasing the orderer at the end
      Future<Object> putEvictedKeyFuture = fork(() -> cache.getAdvancedCache().withFlags(Flag.SKIP_CACHE_LOAD)
                                                           .put(key, value));

      // Confirm the entry has been inserted in the data container so we can evict
      eventually(() -> orderer.getCurrentStage(key) != delayFuture1);
      CompletionStage<DataOperationOrderer.Operation> putEvictedKeyActivationStage = orderer.getCurrentStage(key);

      // Acquire the write orderer for evicted-key again after put(evicted-key) releases it
      CompletableFuture<DataOperationOrderer.Operation> delayFuture2 =
            acquireOrderer(orderer, key, putEvictedKeyActivationStage);
      log.tracef("delayFuture2=%s", delayFuture2.toString());

      // Let put(evicted-key) acquire the orderer and activate evicted-key
      orderer.completeOperation(key, delayFuture1, DataOperationOrderer.Operation.READ);
      putEvictedKeyFuture.get(10, SECONDS);
      assertTrue(putEvictedKeyActivationStage.toCompletableFuture().isDone());

      // delayFuture2 blocks the eviction of evicted-key, but it does not prevent put(other-key) from finishing
      cache.put("other-key", "other-value");
      CompletionStage<DataOperationOrderer.Operation> putOtherKeyPassivationStage = orderer.getCurrentStage(key);
      assertNotSame(delayFuture2, putOtherKeyPassivationStage);

      // Acquire the write orderer for evicted-key again after put(other-key) releases it
      CompletableFuture<DataOperationOrderer.Operation> delayFuture3 =
            acquireOrderer(orderer, key, putOtherKeyPassivationStage);
      log.tracef("delayFuture3=%s", delayFuture3.toString());

      // delayFuture2 is still holding evicted-key's orderer
      // Start get(evicted-key); it cannot complete yet, but it does register a new orderer stage
      Future<Object> getFuture = fork(() -> cache.get(key));
      eventually(() -> orderer.getCurrentStage(key) != putOtherKeyPassivationStage);
      CompletionStage<DataOperationOrderer.Operation> getEvictedKeyActivationStage = orderer.getCurrentStage(key);
      assertFalse(getFuture.isDone());

      // Complete delayFuture2 to release the orderer, it will be acquired by put(other-key)
      orderer.completeOperation(key, delayFuture2, putEvictedKeyActivationStage.toCompletableFuture().join());

      // get(evicted-key) can't finish yet because of delayFuture3
      TestingUtil.assertNotDone(getFuture);

      // Let get(evicted-key) acquire the orderer and finish the activation
      orderer.completeOperation(key, delayFuture3, putOtherKeyPassivationStage.toCompletableFuture().join());

      assertEquals(value, getFuture.get(10, SECONDS));

      // Wait for the activation to finish
      eventuallyEquals(null, () -> orderer.getCurrentStage(key));
      assertTrue(getEvictedKeyActivationStage.toCompletableFuture().isDone());

      // #1 evicted-key evicted by other-key from write
      // #2 other-key evicted by evicted-key from the get
      assertEquals(2L, extractComponent(cache, PassivationManager.class).getPassivations());
      // #1 evicted key activated from the get
      assertEquals(1L, extractComponent(cache, ActivationManager.class).getActivationCount());
      assertEquals(0L, extractComponent(cache, ActivationManager.class).getPendingActivationCount());
   }

   private CompletableFuture<DataOperationOrderer.Operation> acquireOrderer(DataOperationOrderer orderer, String key,
                                                                            CompletionStage<DataOperationOrderer.Operation> oldFuture) {
      CompletableFuture<DataOperationOrderer.Operation> newFuture = new CompletableFuture<>();
      CompletionStage<DataOperationOrderer.Operation> currentFuture = orderer.orderOn(key, newFuture);
      assertSame(currentFuture, oldFuture);
      if (currentFuture != null) {
         assertFalse(currentFuture.toCompletableFuture().isDone());
      }
      return newFuture;
   }

   // This test differs from testEvictionDuringWrite in that it simulates an eviction and acquires the
   // caffeine lock, but is unable to acquire the orderer as it is already taken by a write operation. In this case
   // the eviction has removed the entry and the write puts it back - however the passivation should be skipped
   public void testWriteDuringEviction() throws Exception {
      String key = "evicted-key";
      String initialValue = "value";
      cache.put(key, initialValue);

      // Use delayFuture1 to stop eviction from acquiring the orderer
      // It blocks eviction from acquiring orderer - but has entry lock
      DataOperationOrderer orderer = extractComponent(cache, DataOperationOrderer.class);
      CompletableFuture<DataOperationOrderer.Operation> delayFuture1 = acquireOrderer(orderer, key, null);
      log.tracef("delayFuture1=%s", delayFuture1.toString());

      // This will be stuck evicting the key until it can get the orderer
      Future<Object> putFuture = fork(() -> cache.put("other-key", "other-value"));

      eventually(() -> orderer.getCurrentStage(key) != delayFuture1);
      CompletionStage<DataOperationOrderer.Operation> putOtherKeyPassivationStage = orderer.getCurrentStage(key);

      String newValue = "value-2";
      Future<Object> evictedKeyPutFuture = fork(() -> cache.put(key, newValue));

      // Should be blocked waiting on Caffeine lock - but has the orderer
      TestingUtil.assertNotDone(evictedKeyPutFuture);
      assertFalse(putOtherKeyPassivationStage.toCompletableFuture().isDone());

      // Let the eviction finish, which will let the put happen
      orderer.completeOperation(key, delayFuture1, DataOperationOrderer.Operation.READ);

      putFuture.get(10, SECONDS);

      assertEquals(initialValue, evictedKeyPutFuture.get(10, SECONDS));

      assertInMemory(key, newValue);

      PassivationPersistenceManager ppm = (PassivationPersistenceManager) extractComponent(cache, PersistenceManager.class);
      eventuallyEquals(0, ppm::pendingPassivations);

      assertEquals(2L, extractComponent(cache, PassivationManager.class).getPassivations());
   }

   @Override
   protected void configurePersistence(ConfigurationBuilder builder) {
      // Enable stats so we can count passivations
      builder.statistics().enable();
      builder.persistence().passivation(true)
             .addStore(DummyInMemoryStoreConfigurationBuilder.class);
   }
}
