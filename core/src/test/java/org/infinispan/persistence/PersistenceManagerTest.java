package org.infinispan.persistence;

import static org.infinispan.persistence.manager.PersistenceManager.AccessMode.BOTH;
import static org.infinispan.test.Exceptions.expectException;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.marshall.core.MarshalledEntry;
import org.infinispan.marshall.core.MarshalledEntryImpl;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.persistence.manager.PersistenceManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.concurrent.WithinThreadExecutor;
import org.testng.annotations.Test;

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
      StreamingMarshaller marshaller = extractComponent(cache, StreamingMarshaller.class);
      persistenceManager.writeToAllNonTxStores(marshalledEntry("k", "v", marshaller), BOTH);
      //simulates the scenario where, concurrently, the cache is stopping and handling a topology update.
      persistenceManager.stop();
      //the org.infinispan.persistence.dummy.DummyInMemoryStore throws an exception if the process() method is invoked after stopped.
      persistenceManager.processOnAllStores(new WithinThreadExecutor(), null,
            (marshalledEntry, taskContext) -> fail("shouldn't run"),
            true, true);
   }

   public void testStopDuringProcess() throws ExecutionException, InterruptedException, TimeoutException {
      PersistenceManager persistenceManager = extractComponent(cache, PersistenceManager.class);
      StreamingMarshaller marshaller = extractComponent(cache, StreamingMarshaller.class);
      //simulates the scenario where, concurrently, the cache is stopped during a process loop
      persistenceManager.writeToAllNonTxStores(marshalledEntry("k1", "v1", marshaller), BOTH);
      persistenceManager.writeToAllNonTxStores(marshalledEntry("k2", "v2", marshaller), BOTH);
      persistenceManager.writeToAllNonTxStores(marshalledEntry("k3", "v3", marshaller), BOTH);
      final CountDownLatch before = new CountDownLatch(1);
      final CountDownLatch after = new CountDownLatch(1);
      final AtomicInteger count = new AtomicInteger(0);
      Future<Void> c = fork(() -> persistenceManager.processOnAllStores(new WithinThreadExecutor(), null,
            (marshalledEntry, taskContext) -> {
               before.countDown();
               after.await();
               count.incrementAndGet();
            },
            true, true));
      before.await(30, TimeUnit.SECONDS);
      Future<Void> stopFuture = fork(persistenceManager::stop);
      //stop is unable to proceed while the process isn't finish.
      expectException(TimeoutException.class, () -> stopFuture.get(1, TimeUnit.SECONDS));
      after.countDown();
      c.get(30, TimeUnit.SECONDS);
      stopFuture.get(30, TimeUnit.SECONDS);
      assertEquals(3, count.get());
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   private <K, V> MarshalledEntry<K, V> marshalledEntry(K key, V value, StreamingMarshaller marshaller) {
      return new MarshalledEntryImpl<>(key, value, null, marshaller);
   }
}
