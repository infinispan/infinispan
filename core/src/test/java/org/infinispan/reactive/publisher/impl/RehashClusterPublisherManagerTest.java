package org.infinispan.reactive.publisher.impl;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.distribution.MagicKey;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.ExceptionRunnable;
import org.infinispan.test.Mocks;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Class that tests various rehash scenarios for ClusterPublisherManager to ensure it handles these cases properly
 * @author wburns
 * @since 10.0
 */
@Test(groups = "functional", testName = "reactive.publisher.impl.RehashClusterPublisherManagerTest")
public class RehashClusterPublisherManagerTest extends MultipleCacheManagersTest {

   private static final int[][] START_SEGMENT_OWNERS = new int[][]{{0, 1}, {1, 2}, {2, 3}, {3, 0}};

   protected ControlledConsistentHashFactory factory;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builderUsed = new ConfigurationBuilder();
      factory = new ControlledConsistentHashFactory.Default(START_SEGMENT_OWNERS);
      builderUsed.clustering().cacheMode(CacheMode.DIST_SYNC).hash().consistentHashFactory(factory).numSegments(4);
      createClusteredCaches(4, builderUsed);
   }

   @BeforeMethod
   protected void beforeMethod() throws Exception {
      // Make sure to reset the segments back to original
      factory.setOwnerIndexes(START_SEGMENT_OWNERS);
      factory.triggerRebalance(cache(0));
      TestingUtil.waitForNoRebalance(caches());
   }

   @DataProvider(name = "GuaranteeParallelEntry")
   public Object[][] collectionAndVersionsProvider() {
      return Arrays.stream(DeliveryGuarantee.values())
            .flatMap(dg -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                  .flatMap(parallel -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(entry -> new Object[]{dg, parallel, entry })))
            .toArray(Object[][]::new);
   }

   private void triggerRebalanceSegment2MovesToNode0() throws Exception {
      // Notice that node2 lost the 2 segment and now node0 is the primary
      // {0, 1}, {1, 2}, {2, 3}, {3, 0} - before
      // {0, 1}, {1, 2}, {0, 3}, {3, 0} - after
      factory.setOwnerIndexes(new int[][]{{0, 1}, {1, 2}, {0, 3}, {3, 0}});
      factory.triggerRebalance(cache(0));
      TestingUtil.waitForNoRebalance(caches());
   }

   Function<Map<MagicKey, Object>, Set<MagicKey>> toKeys(boolean useKeys) {
      if (useKeys) {
         return map -> {
            Set<MagicKey> set = new HashSet<>();
            map.keySet().stream()
                  // We purposely don't provide the key that maps to segment 1 to make sure it isn't returned
                  .filter(key -> key.getSegment() != 1)
                  .forEach(set::add);
            // We also add a key that isn't in the cache to make sure it doesn't break stuff
            set.add(new MagicKey(cache(3)));
            return set;
         };
      } else {
         return map -> null;
      }
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testSegmentMovesToOriginatorWhileRetrievingPublisher(DeliveryGuarantee deliveryGuarantee, boolean parallel,
         boolean isEntry) throws Exception {
      Cache cache2 = cache(2);

      CheckPoint checkPoint = new CheckPoint();
      // Always let it finish once released
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      // Block on the checkpoint when it is requesting segment 2 from node 2
      Mocks.blockingMock(checkPoint, InternalDataContainer.class, cache2,
            (stub, m) -> stub.when(m).publisher(Mockito.eq(2)));

      int expectedAmount = caches().size();
      // If it is at most once, we don't retry the segment so the count will be off by 1
      if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE) {
         expectedAmount -= 1;
      }

      runCommand(deliveryGuarantee, parallel, isEntry, expectedAmount, () -> {
         assertTrue(checkPoint.await(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS));

         triggerRebalanceSegment2MovesToNode0();

         checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      });
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testSegmentMovesToOriginatorJustBeforeSendingRemoteKey(DeliveryGuarantee deliveryGuarantee, boolean parallel,
         boolean isEntry) throws Exception {
      testSegmentMovesToOriginatorJustBeforeSendingRemote(deliveryGuarantee, parallel, isEntry, true);
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testSegmentMovesToOriginatorJustBeforeSendingRemoteNoKey(DeliveryGuarantee deliveryGuarantee, boolean parallel,
         boolean isEntry) throws Exception {
      testSegmentMovesToOriginatorJustBeforeSendingRemote(deliveryGuarantee, parallel, isEntry, false);
   }

   private void testSegmentMovesToOriginatorJustBeforeSendingRemote(DeliveryGuarantee deliveryGuarantee, boolean parallel,
         boolean isEntry, boolean useKeys) throws Exception {
      Cache cache0 = cache(0);
      Address cache2Address = address(2);

      CheckPoint checkPoint = new CheckPoint();
      // Always let it finish once released
      checkPoint.triggerForever(Mocks.AFTER_RELEASE);

      // Block on about to send the remote command to node2
      RpcManager original = Mocks.blockingMock(checkPoint, RpcManager.class, cache0,
            (stub, m) -> stub.when(m).invokeCommand(eq(cache2Address), isA(PublisherRequestCommand.class), any(), any()));

      int expectedAmount = caches().size();
      // If it is at most once, we don't retry the segment so the count will be off by 1
      if (deliveryGuarantee == DeliveryGuarantee.AT_MOST_ONCE) {
         expectedAmount -= 1;
      }

      // When we are using keys, we explicitly don't pass one of them
      if (useKeys) {
         expectedAmount--;
      }

      try {
         runCommand(deliveryGuarantee, parallel, isEntry, expectedAmount, () -> {
            assertTrue(checkPoint.await(Mocks.BEFORE_INVOCATION, 10, TimeUnit.SECONDS));

            triggerRebalanceSegment2MovesToNode0();

            checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
         }, toKeys(useKeys));
      } finally {
         if (original != null) {
            TestingUtil.replaceComponent(cache0, RpcManager.class, original, true);
         }
      }
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testSegmentMovesToOriginatorJustBeforePublisherCompletes(DeliveryGuarantee deliveryGuarantee, boolean parallel,
         boolean isEntry) throws Exception {
      Cache cache2 = cache(2);

      CheckPoint checkPoint = new CheckPoint();
      // Always let it process the publisher
      checkPoint.triggerForever(Mocks.BEFORE_RELEASE);
      // Block on the publisher
      InternalDataContainer internalDataContainer = TestingUtil.extractComponent(cache2, InternalDataContainer.class);
      InternalDataContainer spy = spy(internalDataContainer);

      doAnswer(invocation -> {
         Publisher result = (Publisher) invocation.callRealMethod();
         return Mocks.blockingPublisher(result, checkPoint);
      }).when(spy).publisher(eq(2));

      TestingUtil.replaceComponent(cache2, InternalDataContainer.class, spy, true);


      int expectedAmount = caches().size();
      // At least once is retried after retrieving the value, so it will count the value for segment 2 twice
      if (deliveryGuarantee == DeliveryGuarantee.AT_LEAST_ONCE) {
         expectedAmount++;
      }

      runCommand(deliveryGuarantee, parallel, isEntry, expectedAmount, () -> {
         // Wait until after the publisher completes - but don't let it just yet
         assertTrue(checkPoint.await(Mocks.AFTER_INVOCATION, 10, TimeUnit.SECONDS));

         triggerRebalanceSegment2MovesToNode0();

         checkPoint.triggerForever(Mocks.AFTER_RELEASE);
      });
   }

   private void runCommand(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry, int expectedAmount,
         ExceptionRunnable performOperation) throws Exception {
      runCommand(deliveryGuarantee, parallel, isEntry, expectedAmount, performOperation, map -> null);
   }

   private void runCommand(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry, int expectedAmount,
         ExceptionRunnable performOperation, Function<Map<MagicKey, Object>, Set<MagicKey>> keysHandler) throws Exception {
      Map<MagicKey, Object> entries = new HashMap<>();
      // Insert a key into each cache - so we always have values to find on each node - means we have 1 entry per segment as well
      for (Cache<MagicKey, Object> cache : this.<MagicKey, Object>caches()) {
         MagicKey key = new MagicKey(cache);
         Object value = key.toString();
         cache.put(key, value);
         entries.put(key, value);
      }

      Set<MagicKey> keys = keysHandler.apply(entries);

      // We always fork, as some methods may block a resource that is invoked on the main thread
      Future<CompletionStage<Long>> future = fork(() -> {
         ClusterPublisherManager<MagicKey, String> cpm = TestingUtil.extractComponent(cache(0), ClusterPublisherManager.class);
         CompletionStage<Long> stageCount;
         if (isEntry) {
            stageCount = cpm.entryReduction(parallel, null, keys, null, false, deliveryGuarantee,
                  PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
         } else {
            stageCount = cpm.keyReduction(parallel, null, keys, null, false, deliveryGuarantee,
                  PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
         }
         return stageCount;
      });

      performOperation.run();

      Long actualCount = future.get(10, TimeUnit.MINUTES)
            .toCompletableFuture().get(10, TimeUnit.MINUTES);
      // Should be 1 entry per node
      assertEquals(expectedAmount, actualCount.intValue());
   }
}
