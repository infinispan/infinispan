package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.reactive.publisher.impl.commands.reduction.PublisherResult;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.concurrent.BlockingManager;
import org.infinispan.util.concurrent.CompletionStages;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.functions.BiFunction;

@Test(groups = "functional", testName = "reactive.publisher.impl.SimpleLocalPublisherManagerTest")
@InCacheMode({CacheMode.REPL_SYNC, CacheMode.DIST_SYNC})
public class SimpleLocalPublisherManagerTest extends MultipleCacheManagersTest {
   private static final int SEGMENT_COUNT = 128;

   ConfigurationBuilder cacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, false);
      builder.clustering().hash().numSegments(SEGMENT_COUNT);
      return builder;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = cacheConfiguration();
      createCluster(builder, 3);
      waitForClusterToForm();
   }

   private Map<Integer, String> insert(Cache<Integer, String> cache) {
      int amount = 100;
      Map<Integer, String> values = new HashMap<>(amount);

      IntStream.range(0, amount).forEach(i -> values.put(i, "value-" + i));
      cache.putAll(values);
      return values;
   }

   private LocalPublisherManager<Integer, String> lpm(Cache<Integer, String> cache) {
      return TestingUtil.extractComponent(cache, LocalPublisherManager.class);
   }

   @DataProvider(name = "GuaranteeEntry")
   public Object[][] deliveryGuaranteeAndEntryProvider() {
      return Arrays.stream(DeliveryGuarantee.values())
            .flatMap(dg -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                  .map(entry -> new Object[]{dg, entry}))
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testNoIntermediateOps(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> inserted = insert(cache);

      LocalPublisherManager<Integer, String> lpm = lpm(cache);
      IntSet allSegments = IntSets.immutableRangeSet(SEGMENT_COUNT);
      SegmentAwarePublisherSupplier<?> publisher;
      Consumer<Object> assertConsumer;
      if (isEntry) {
         publisher = lpm.keyPublisher(allSegments, null, null, 0L,
               deliveryGuarantee, Function.identity());
         assertConsumer = obj -> assertTrue(inserted.containsKey(obj));
      } else {
         publisher = lpm.entryPublisher(allSegments, null, null, 0L,
               deliveryGuarantee, Function.identity());
         assertConsumer = obj -> {
            Map.Entry<Object, Object> entry = (Map.Entry) obj;
            Object value = inserted.get(entry.getKey());
            assertEquals(value, entry.getValue());
         };
      }

      DistributionManager dm = TestingUtil.extractComponent(cache, DistributionManager.class);
      IntSet localSegments = dm.getCacheTopology().getLocalReadSegments();

      int expected = SimpleClusterPublisherManagerTest.findHowManyInSegments(inserted.size(), localSegments, TestingUtil.extractComponent(cache, KeyPartitioner.class));

      Set<Object> results = Flowable.fromPublisher(publisher.publisherWithoutSegments())
            .collectInto(new HashSet<>(), HashSet::add)
            .blockingGet();
      assertEquals(expected, results.size());

      results.forEach(assertConsumer);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testProperOrderingGuarantees(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> inserted = insert(cache);

      LocalPublisherManager<Integer, String> lpm = lpm(cache);
      IntSet allSegments = IntSets.immutableRangeSet(SEGMENT_COUNT);
      SegmentAwarePublisherSupplier<?> publisher;
      if (isEntry) {
         publisher = lpm.keyPublisher(allSegments, null, null, 0L,
               deliveryGuarantee, Function.identity());
      } else {
         publisher = lpm.entryPublisher(allSegments, null, null, 0L,
               deliveryGuarantee, Function.identity());
      }

      DistributionManager dm = TestingUtil.extractComponent(cache, DistributionManager.class);
      IntSet localSegments = dm.getCacheTopology().getLocalReadSegments();

      int expected = SimpleClusterPublisherManagerTest.findHowManyInSegments(inserted.size(), localSegments,
            TestingUtil.extractComponent(cache, KeyPartitioner.class));

      List<?> list = Flowable.fromPublisher(publisher.publisherWithLostSegments())
            .collect(Collectors.toList())
            .blockingGet();
      assertEquals(expected + SEGMENT_COUNT, list.size());

      int currentSegment = -1;

      for (Object obj : list) {
         SegmentAwarePublisherSupplier.NotificationWithLost<Object> notification =
               (SegmentAwarePublisherSupplier.NotificationWithLost<Object>) obj;
         if (notification.isValue()) {
            if (currentSegment == -1) {
               currentSegment = notification.valueSegment();
            } else {
               assertEquals(currentSegment, notification.valueSegment());
            }
         } else {
            int segment;
            if (notification.isSegmentComplete()) {
               segment = notification.completedSegment();
               if (!localSegments.contains(segment)) {
                  assertEquals("Only at most once can say the segment is complete without having it", deliveryGuarantee, DeliveryGuarantee.AT_MOST_ONCE);
               }
            } else {
               segment = notification.lostSegment();
               assertFalse(localSegments.contains(segment));
            }

            if (currentSegment != -1) {
               assertEquals(currentSegment, notification.completedSegment());
               currentSegment = -1;
            }
         }
      }
   }

   @DataProvider(name = "GuaranteeParallelEntry")
   public Object[][] deliveryGuaranteeParallelEntryProvider() {
      return Arrays.stream(DeliveryGuarantee.values())
            .flatMap(dg -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                  .flatMap(parallel -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(entry -> new Object[]{dg, parallel, entry}))
            )
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testWithAsyncOperation(DeliveryGuarantee deliveryGuarantee, boolean isParallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> inserted = insert(cache);

      BlockingManager blockingManager = TestingUtil.extractComponent(cache, BlockingManager.class);

      LocalPublisherManager<Integer, String> lpm = lpm(cache);
      IntSet allSegments = IntSets.immutableRangeSet(SEGMENT_COUNT);
      CompletionStage<PublisherResult<Set<Object>>> stage;
      Consumer<Object> assertConsumer;
      Collector<Object, ?, Set<Object>> collector = Collectors.toSet();
      BiFunction<Set<Object>, Set<Object>, Set<Object>> reduceBiFunction = (left, right) -> {
         left.addAll(right);
         return left;
      };

      io.reactivex.rxjava3.functions.Function<Object, Single<Object>> sleepOnBlockingPoolFunction = value ->
            Single.fromCompletionStage(blockingManager.supplyBlocking(() -> value, "test-blocking-thread"));

      if (isEntry) {
         stage = lpm.keyReduction(isParallel, allSegments, null, null, 0L, deliveryGuarantee,
               publisher -> Flowable.fromPublisher(publisher)
                     .concatMapSingle(sleepOnBlockingPoolFunction)
                     .collect(collector)
                     .toCompletionStage(),
               publisher -> Flowable.fromPublisher(publisher)
                     .reduce(reduceBiFunction)
                     .toCompletionStage(Collections.emptySet()));
         assertConsumer = obj -> assertTrue(inserted.containsKey(obj));
      } else {
         stage = lpm.entryReduction(isParallel, allSegments, null, null, 0L,
               deliveryGuarantee, publisher -> Flowable.fromPublisher(publisher)
                     .concatMapSingle(sleepOnBlockingPoolFunction).collect(collector).toCompletionStage()
               , publisher -> Flowable.fromPublisher(publisher)
                     .reduce(reduceBiFunction).toCompletionStage(Collections.emptySet()));
         assertConsumer = obj -> {
            Map.Entry<Object, Object> entry = (Map.Entry) obj;
            Object value = inserted.get(entry.getKey());
            assertEquals(value, entry.getValue());
         };
      }

      DistributionManager dm = TestingUtil.extractComponent(cache, DistributionManager.class);
      IntSet localSegments = dm.getCacheTopology().getLocalReadSegments();

      int expected = SimpleClusterPublisherManagerTest.findHowManyInSegments(inserted.size(), localSegments, TestingUtil.extractComponent(cache, KeyPartitioner.class));

      Set<Object> results = CompletionStages.join(stage).getResult();
      assertEquals(expected, results.size());

      results.forEach(assertConsumer);
   }
}
