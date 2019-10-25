package org.infinispan.persistence;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.persistence.manager.PersistenceManagerImpl;
import org.infinispan.test.Exceptions;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.CompletionStages;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.internal.functions.Functions;
import io.reactivex.subscribers.TestSubscriber;

/**
 * A {@link PersistenceManager} unit test.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "unit", testName = "persistence.PersistenceManagerTest")
@CleanupAfterMethod
public class PersistenceManagerTest extends SingleCacheManagerTest {

   public void testProcessAfterStop() {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      String key = "k";
      persistenceManager.writeToAllNonTxStores(MarshalledEntryUtil.create(key, "v", cache), keyPartitioner.getSegment(key), BOTH);
      //simulates the scenario where, concurrently, the cache is stopping and handling a topology update.
      persistenceManager.stop();
      //the org.infinispan.persistence.dummy.DummyInMemoryStore throws an exception if the process() method is invoked after stopped.
      Flowable.fromPublisher(persistenceManager.publishEntries(true, true)).subscribe(ignore -> fail("shouldn't run"));
   }

   public void testStopDuringProcess() throws ExecutionException, InterruptedException, TimeoutException {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      //simulates the scenario where, concurrently, the cache is stopped during a process loop
      CompletionStages.join(persistenceManager.writeToAllNonTxStores(MarshalledEntryUtil.create("k1", "v1", cache), keyPartitioner.getSegment("k1"), BOTH));
      CompletionStages.join(persistenceManager.writeToAllNonTxStores(MarshalledEntryUtil.create("k2", "v2", cache), keyPartitioner.getSegment("k2"), BOTH));
      CompletionStages.join(persistenceManager.writeToAllNonTxStores(MarshalledEntryUtil.create("k3", "v3", cache), keyPartitioner.getSegment("k3"), BOTH));
      final CountDownLatch before = new CountDownLatch(1);
      final CountDownLatch after = new CountDownLatch(1);
      Future<Integer> c = fork(() -> {
         TestSubscriber<Object> subscriber = TestSubscriber.create(0);
         Flowable.fromPublisher(persistenceManager.publishEntries(true, true))
               .subscribe(subscriber);
         before.countDown();
         assertTrue(after.await(10, TimeUnit.SECONDS));
         // request all the elements after we have initiated stop (3 elements with 100ms wait for each will be run)
         subscriber.request(Long.MAX_VALUE);
         subscriber.await(10, TimeUnit.SECONDS);
         subscriber.assertNoErrors();
         subscriber.assertComplete();
         return subscriber.valueCount();
      });
      assertTrue(before.await(30, TimeUnit.SECONDS));
      Future<Void> stopFuture = fork(persistenceManager::stop);
      //stop is unable to proceed while the process isn't finish - note that with slow store the publisher should take 300+ ms
      expectException(TimeoutException.class, () -> stopFuture.get(50, TimeUnit.MILLISECONDS));
      after.countDown();
      Integer count = c.get(30, TimeUnit.SECONDS);
      stopFuture.get(30, TimeUnit.SECONDS);
      assertEquals(3, count.intValue());
   }

   public void testEarlyTerminatedOperation() {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      // This has to be > 128 - as the flowable pulls a chunk of that size
      for (int i = 0; i < 140; ++i) {
         String key = "k" + i;
         persistenceManager.writeToAllNonTxStores(MarshalledEntryUtil.create(key, "v", cache), keyPartitioner.getSegment(key), BOTH);
      }
      PersistenceManagerImpl pmImpl = (PersistenceManagerImpl) persistenceManager;
      assertEquals(0, pmImpl.activePublisherInvocations());
      assertFalse(cache.isEmpty());
      assertEquals(0, pmImpl.activePublisherInvocations());
   }

   public void testPublishWithInterrupt() throws InterruptedException {
      ExecutorService original = TestingUtil.extractGlobalComponent(cache.getCacheManager(), ExecutorService.class, KnownComponentNames.PERSISTENCE_EXECUTOR);
      // Use within thread so we can interrupt ourselves
      TestingUtil.replaceComponent(cache.getCacheManager(), ExecutorService.class, KnownComponentNames.PERSISTENCE_EXECUTOR, new WithinThreadExecutor(), true);
      try {
         PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
         persistenceManager.stop();
         persistenceManager.start();

         Thread.currentThread().interrupt();

         // The throwable is observed on the cpu thread - so wait for it
         CountDownLatch latch = new CountDownLatch(1);
         AtomicReference<Throwable> errorCatcher = new AtomicReference<>();
         Flowable.fromPublisher(persistenceManager.publishEntries(true, true))
               .subscribe(Functions.emptyConsumer(), t -> {
                  errorCatcher.set(t);
                  latch.countDown();
               });

         latch.await(10, TimeUnit.SECONDS);

         Throwable t = errorCatcher.get();
         Exceptions.assertException(InterruptedException.class, t);
      } finally {
         TestingUtil.replaceComponent(cache.getCacheManager(), ExecutorService.class, KnownComponentNames.PERSISTENCE_EXECUTOR, original, true);
      }
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).slow(true);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
