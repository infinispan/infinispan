package org.infinispan.persistence.manager;

import static org.infinispan.commons.test.Exceptions.expectCompletionException;
import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.getFirstStore;
import static org.infinispan.test.TestingUtil.getStore;
import static org.infinispan.util.concurrent.CompletionStages.join;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.persistence.impl.MarshalledEntryUtil;
import org.infinispan.persistence.support.DelayStore;
import org.infinispan.persistence.support.FailStore;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestException;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;

/**
 * A {@link PersistenceManager} unit test.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
@Test(groups = "unit", testName = "persistence.PersistenceManagerTest")
@CleanupAfterMethod
public class PersistenceManagerTest extends SingleCacheManagerTest {

   /**
    * Simulates cache receiving a topology update while stopping.
    */
   public void testPublishAfterStop() {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      String key = "k";
      insertEntry(persistenceManager, keyPartitioner, key, "v");

      persistenceManager.stop();

      // The stopped PersistenceManager should never pass the request to the store
      Flowable.fromPublisher(persistenceManager.publishEntries(true, true))
              .blockingSubscribe(ignore -> fail("shouldn't run"));
   }

   /**
    * Simulates cache stopping while processing a state request.
    */
   public void testStopDuringPublish() throws ExecutionException, InterruptedException, TimeoutException {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      insertEntry(persistenceManager, keyPartitioner, "k1", "v1");
      insertEntry(persistenceManager, keyPartitioner, "k2", "v2");
      insertEntry(persistenceManager, keyPartitioner, "k3", "v3");

      DelayStore store = getFirstStore(cache);
      store.delayBeforeEmit(1);

      CountDownLatch before = new CountDownLatch(1);
      CountDownLatch after = new CountDownLatch(1);
      Future<Integer> publisherFuture = fork(() -> {
         TestSubscriber<Object> subscriber = TestSubscriber.create(0);
         Flowable.fromPublisher(persistenceManager.publishEntries(true, true))
                 .subscribe(subscriber);
         before.countDown();
         assertTrue(after.await(10, TimeUnit.SECONDS));
         // request all the elements after we have initiated stop
         subscriber.request(Long.MAX_VALUE);
         subscriber.await(10, TimeUnit.SECONDS);
         subscriber.assertNoErrors();
         subscriber.assertComplete();
         return subscriber.values().size();
      });

      assertTrue(before.await(30, TimeUnit.SECONDS));
      Future<Void> stopFuture = fork(persistenceManager::stop);
      // Stop is unable to proceed while the publisher hasn't completed
      Thread.sleep(50);
      assertFalse(stopFuture.isDone());
      assertFalse(publisherFuture.isDone());

      after.countDown();

      // Publisher can't continue because the store emit is delayed
      Thread.sleep(50);
      assertFalse(stopFuture.isDone());
      assertFalse(publisherFuture.isDone());

      // Emit the entries and allow PMI to stop
      store.endDelay();
      Integer count = publisherFuture.get(30, TimeUnit.SECONDS);
      stopFuture.get(30, TimeUnit.SECONDS);
      assertEquals(3, count.intValue());
   }

   public void testEarlyTerminatedPublish() {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      // This has to be > 128 - as the flowable pulls a chunk of that size
      for (int i = 0; i < 140; ++i) {
         String key = "k" + i;
         insertEntry(persistenceManager, keyPartitioner, key, "v");
      }

      DelayStore store = getFirstStore(cache);
      store.delayBeforeEmit(1);

      PersistenceManagerImpl pmImpl = (PersistenceManagerImpl) persistenceManager;
      assertFalse(pmImpl.anyLocksHeld());
      assertFalse(cache.isEmpty());
      assertFalse(pmImpl.anyLocksHeld());

      store.endDelay();
   }

   public void testStoreExceptionInWrite() {
      PersistenceManager pm = extractComponent(cache, PersistenceManager.class);
      KeyPartitioner keyPartitioner = extractComponent(cache, KeyPartitioner.class);
      DelayStore store1 = getStore(cache, 0, true);
      FailStore store2 = getStore(cache, 1, true);
      store2.failModification(2);

      String key = "k";
      int segment = keyPartitioner.getSegment(key);
      expectCompletionException(TestException.class,
                                pm.writeToAllNonTxStores(MarshalledEntryUtil.create(key, "v", cache), segment, BOTH));
      assertTrue(store1.contains(key));

      expectCompletionException(TestException.class, pm.deleteFromAllStores(key, segment, BOTH));
      assertFalse(store1.contains(key));
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.persistence().addStore(DelayStore.ConfigurationBuilder.class);
      cfg.persistence().addStore(FailStore.ConfigurationBuilder.class);
      cfg.persistence().addStore(FailStore.ConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   private void insertEntry(PersistenceManager persistenceManager, KeyPartitioner keyPartitioner, String k, String v) {
      join(persistenceManager.writeToAllNonTxStores(MarshalledEntryUtil.create(k, v, cache),
                                                    keyPartitioner.getSegment(k), BOTH));
   }
}
