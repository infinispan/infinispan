package org.infinispan.functional.distribution.rehash;

import static org.infinispan.test.TestingUtil.withTx;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiFunction;

import org.infinispan.Cache;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.reactive.publisher.impl.commands.batch.PublisherResponse;
import org.infinispan.remoting.responses.SuccessfulObjResponse;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.impl.RequestRepository;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.testng.annotations.Test;

import jakarta.transaction.Transaction;

@Test(groups = "functional", testName = "functional.distribution.rehash.FunctionalTxTest")
@CleanupAfterMethod
public class FunctionalTxTest extends MultipleCacheManagersTest {
   ConfigurationBuilder cb;
   ControlledConsistentHashFactory chf;

   @Override
   protected void createCacheManagers() throws Throwable {
      chf = new ControlledConsistentHashFactory.Default(0, 1);
      cb = new ConfigurationBuilder();
      cb.transaction().transactionMode(TransactionMode.TRANSACTIONAL).useSynchronization(false);
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numSegments(1);
      cb.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(chf);

      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, cb, 3);
      waitForClusterToForm();
   }

   public void testDoubleIncrementBeforeTopology() throws Exception {
      testBeforeTopology((rw, key) -> {
         Integer oldValue = rw.eval(key, FunctionalTxTest::increment).join();
         rw.eval(key, FunctionalTxTest::increment).join();
         return oldValue;
      }, 2);
   }

   public void testDoubleIncrementAfterTopology() throws Exception {
      testAfterTopology((rw, key) -> {
         Integer oldValue = rw.eval(key, FunctionalTxTest::increment).join();
         rw.eval(key, FunctionalTxTest::increment).join();
         return oldValue;
      }, 2);
   }

   public void testReadWriteKeyBeforeTopology() throws Exception {
      testBeforeTopology((rw, key) -> rw.eval(key, FunctionalTxTest::increment).join(), 1);
   }

   public void testReadWriteKeyAfterTopology() throws Exception {
      testAfterTopology((rw, key) -> rw.eval(key, FunctionalTxTest::increment).join(), 1);
   }

   public void testReadWriteManyKeysBeforeTopology() throws Exception {
      testBeforeTopology((rw, key) -> rw.evalMany(Collections.singleton(key), FunctionalTxTest::increment).findAny().get(), 1);
   }

   public void testReadWriteManyKeysAfterTopology() throws Exception {
      testAfterTopology((rw, key) -> rw.evalMany(Collections.singleton(key), FunctionalTxTest::increment).findAny().get(), 1);
   }

   public void testReadWriteManyEntriesBeforeTopology() throws Exception {
      testBeforeTopology((rw, key) -> rw.evalMany(Collections.singletonMap(key, 1), FunctionalTxTest::add).findAny().get(), 1);
   }

   public void testReadWriteManyEntriesAfterTopology() throws Exception {
      testAfterTopology((rw, key) -> rw.evalMany(Collections.singletonMap(key, 1), FunctionalTxTest::add).findAny().get(), 1);
   }

   private void testBeforeTopology(BiFunction<FunctionalMap.ReadWriteMap<String, Integer>, String, Integer> op, int expectedIncrement) throws Exception {
      cache(0).put("key", 1);

      // Blocking on receiver side. We cannot block the StateResponseCommand on the server side since
      // the InternalCacheEntries in its state are the same instances of data stored in DataContainer
      // - therefore when the command is blocked on sender the command itself would be mutated by applying
      // the transaction below.
      CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      CountDownLatch applyStateStartedLatch = new CountDownLatch(1);
      blockStateTransfer(cache(2), applyStateStartedLatch, applyStateProceedLatch);

      tm(2).begin();
      FunctionalMap.ReadWriteMap<String, Integer> rw = FunctionalMap.create(this.<String, Integer>cache(2).getAdvancedCache()).toReadWriteMap();
      assertEquals(Integer.valueOf(1), op.apply(rw, "key"));
      Transaction tx = tm(2).suspend();

      chf.setOwnerIndexes(0, 2);

      EmbeddedCacheManager cm = createClusteredCacheManager(false, ControlledConsistentHashFactory.SCI.INSTANCE,
            cb, new TransportFlags());
      registerCacheManager(cm);
      Future<?> future = fork(() -> {
         cm.start();
         cache(3);
      });

      try {
         assertTrue(applyStateStartedLatch.await(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }

      DistributionInfo distributionInfo = cache(2).getAdvancedCache().getDistributionManager().getCacheTopology().getDistribution("key");
      assertFalse(distributionInfo.isReadOwner());
      assertTrue(distributionInfo.isWriteBackup());

      tm(2).resume(tx);
      tm(2).commit();

      applyStateProceedLatch.countDown();

      future.get(10, TimeUnit.SECONDS);

      InternalCacheEntry<Object, Object> ice = cache(2).getAdvancedCache().getDataContainer().peek("key");
      assertEquals("Current ICE: " + ice, 1 + expectedIncrement, ice.getValue());
   }

   private void testAfterTopology(BiFunction<FunctionalMap.ReadWriteMap<String, Integer>, String, Integer> op, int expectedIncrement) throws Exception {
      cache(0).put("key", 1);

      // Blocking on receiver side. We cannot block the StateResponseCommand on the server side since
      // the InternalCacheEntries in its state are the same instances of data stored in DataContainer
      // - therefore when the command is blocked on sender the command itself would be mutated by applying
      // the transaction below.
      CountDownLatch applyStateProceedLatch = new CountDownLatch(1);
      CountDownLatch applyStateStartedLatch = new CountDownLatch(1);
      blockStateTransfer(cache(2), applyStateStartedLatch, applyStateProceedLatch);

      chf.setOwnerIndexes(0, 2);
      EmbeddedCacheManager cm = createClusteredCacheManager(false, ControlledConsistentHashFactory.SCI.INSTANCE,
            cb, new TransportFlags());
      registerCacheManager(cm);
      Future<?> future = fork(() -> {
         cm.start();
         cache(3);
      });

      try {
         assertTrue(applyStateStartedLatch.await(10, TimeUnit.SECONDS));
      } catch (InterruptedException e) {
         throw new RuntimeException(e);
      }

      DistributionInfo distributionInfo = cache(2).getAdvancedCache().getDistributionManager().getCacheTopology()
            .getDistribution("key");
      assertFalse(distributionInfo.isReadOwner());
      assertTrue(distributionInfo.isWriteBackup());

      withTx(tm(2), () -> {
         FunctionalMap.ReadWriteMap<String, Integer> rw = FunctionalMap.create(this.<String, Integer>cache(2).getAdvancedCache()).toReadWriteMap();
         assertEquals(Integer.valueOf(1), op.apply(rw, "key"));
         return null;
      });

      applyStateProceedLatch.countDown();

      future.get(10, TimeUnit.SECONDS);

      InternalCacheEntry<Object, Object> ice = cache(2).getAdvancedCache().getDataContainer().peek("key");
      assertEquals("Current ICE: " + ice, 1 + expectedIncrement, ice.getValue());
   }

   private static Integer increment(EntryView.ReadWriteEntryView<String, Integer> view) {
      int value = view.find().orElse(0);
      view.set(value + 1);
      return value;
   }

   private static Integer add(Integer param, EntryView.ReadWriteEntryView<String, Integer> view) {
      int value = view.find().orElse(0);
      view.set(value + param);
      return value;
   }

   private static void blockStateTransfer(Cache<?,?> cache, CountDownLatch started, CountDownLatch proceed) {
      Transport transport = TestingUtil.extractComponent(cache, Transport.class);
      RequestRepository requestRepository = Mocks.replaceFieldWithSpy(transport, "requests");

      doAnswer(invocation -> {
         started.countDown();
         try {
            if (!proceed.await(15, TimeUnit.SECONDS)) {
               throw CompletableFutures.asCompletionException(new TimeoutException());
            }
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
         return invocation.callRealMethod();
      }).when(requestRepository).addResponse(anyLong(), any(), argThat(r ->
            r.isSuccessful() && r instanceof SuccessfulObjResponse<?> sor && sor.getResponseValue() instanceof PublisherResponse));
   }
}
