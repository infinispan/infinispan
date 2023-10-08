package org.infinispan.functional.distribution.rehash;

import static org.infinispan.test.TestingUtil.withTx;
import static org.infinispan.test.fwk.TestCacheManagerFactory.createClusteredCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.functional.EntryView;
import org.infinispan.functional.FunctionalMap;
import org.infinispan.functional.impl.FunctionalMapImpl;
import org.infinispan.functional.impl.ReadWriteMapImpl;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.DelegatingStateConsumer;
import org.infinispan.statetransfer.StateChunk;
import org.infinispan.statetransfer.StateConsumer;
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
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).hash().numSegments(1).consistentHashFactory(chf);

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
      BlockingStateConsumer bsc2 = TestingUtil.wrapComponent(cache(2), StateConsumer.class, BlockingStateConsumer::new);

      tm(2).begin();
      FunctionalMap.ReadWriteMap<String, Integer> rw = ReadWriteMapImpl.create(
            FunctionalMapImpl.create(this.<String, Integer>cache(2).getAdvancedCache()));
      assertEquals(Integer.valueOf(1), op.apply(rw, "key"));
      Transaction tx = tm(2).suspend();

      chf.setOwnerIndexes(0, 2);

      EmbeddedCacheManager cm = createClusteredCacheManager(false, ControlledConsistentHashFactory.SCI.INSTANCE,
                                                            cb, new TransportFlags());
      registerCacheManager(cm);
      Future<?> future = fork(() -> {
         cache(3);
      });

      bsc2.await();

      DistributionInfo distributionInfo = cache(2).getAdvancedCache().getDistributionManager().getCacheTopology().getDistribution("key");
      assertFalse(distributionInfo.isReadOwner());
      assertTrue(distributionInfo.isWriteBackup());

      tm(2).resume(tx);
      tm(2).commit();

      bsc2.unblock();

      future.get(10, TimeUnit.SECONDS);

      InternalCacheEntry<Object, Object> ice = cache(2).getAdvancedCache().getDataContainer().get("key");
      assertEquals("Current ICE: " + ice, 1 + expectedIncrement, ice.getValue());
   }

   private void testAfterTopology(BiFunction<FunctionalMap.ReadWriteMap<String, Integer>, String, Integer> op, int expectedIncrement) throws Exception {
      cache(0).put("key", 1);

      // Blocking on receiver side. We cannot block the StateResponseCommand on the server side since
      // the InternalCacheEntries in its state are the same instances of data stored in DataContainer
      // - therefore when the command is blocked on sender the command itself would be mutated by applying
      // the transaction below.
      BlockingStateConsumer bsc2 = TestingUtil.wrapComponent(cache(2), StateConsumer.class, BlockingStateConsumer::new);

      chf.setOwnerIndexes(0, 2);
      EmbeddedCacheManager cm = createClusteredCacheManager(false, ControlledConsistentHashFactory.SCI.INSTANCE,
                                                            cb, new TransportFlags());
      registerCacheManager(cm);
      Future<?> future = fork(() -> {
         cache(3);
      });

      bsc2.await();

      DistributionInfo distributionInfo = cache(2).getAdvancedCache().getDistributionManager().getCacheTopology()
                                                  .getDistribution("key");
      assertFalse(distributionInfo.isReadOwner());
      assertTrue(distributionInfo.isWriteBackup());

      withTx(tm(2), () -> {
         FunctionalMap.ReadWriteMap<String, Integer> rw = ReadWriteMapImpl.create(
               FunctionalMapImpl.create(this.<String, Integer>cache(2).getAdvancedCache()));

         assertEquals(Integer.valueOf(1), op.apply(rw, "key"));
         return null;
      });

      bsc2.unblock();

      future.get(10, TimeUnit.SECONDS);

      InternalCacheEntry<Object, Object> ice = cache(2).getAdvancedCache().getDataContainer().get("key");
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

   private static class BlockingStateConsumer extends DelegatingStateConsumer {
      private final CountDownLatch expectLatch = new CountDownLatch(1);
      private final CountDownLatch blockLatch = new CountDownLatch(1);

      public BlockingStateConsumer(StateConsumer delegate) {
         super(delegate);
      }

      @Override
      public CompletionStage<?> applyState(Address sender, int topologyId, Collection<StateChunk> stateChunks) {
         expectLatch.countDown();
         try {
            assertTrue(blockLatch.await(10, TimeUnit.SECONDS));
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
         return super.applyState(sender, topologyId, stateChunks);
      }

      public void await() {
         try {
            assertTrue(expectLatch.await(10, TimeUnit.SECONDS));
         } catch (InterruptedException e) {
            throw new RuntimeException(e);
         }
      }

      public void unblock() {
         blockLatch.countDown();
      }
   }
}
