package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.entries.CacheEntry;
import org.infinispan.container.entries.ImmortalCacheEntry;
import org.infinispan.container.entries.NullCacheEntry;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.NonTxInvocationContext;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.infinispan.util.function.SerializableFunction;
import org.mockito.Mockito;
import org.reactivestreams.Publisher;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.reactivex.Flowable;
import io.reactivex.subscribers.TestSubscriber;

/**
 * Test class for simple ClusterPublisherManager to ensure arguments are adhered to
 * @author wburns
 * @since 10.0
 */
@Test(groups = "functional", testName = "reactive.publisher.impl.SimpleClusterPublisherManagerTest")
@InCacheMode({CacheMode.REPL_SYNC, CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC})
public class SimpleClusterPublisherManagerTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, false);
      // Reduce number of segments a bit
      builder.clustering().hash().numSegments(10);
      createCluster(builder, 4);
      waitForClusterToForm();
   }

   private Map<Integer, String> insert(Cache<Integer, String> cache) {
      int amount = 24;
      Map<Integer, String> values = new HashMap<>(amount);

      Map<Integer, IntSet> keysBySegment = log.isTraceEnabled() ? new HashMap<>() : null;
      KeyPartitioner kp = TestingUtil.extractComponent(cache, KeyPartitioner.class);

      IntStream.range(0, amount).forEach(i -> {
         values.put(i, "value-" + i);
         if (keysBySegment != null) {
            int segment = kp.getSegment(i);
            IntSet keys = keysBySegment.computeIfAbsent(segment, IntSets::mutableEmptySet);
            keys.set(i);
         }
      });
      if (keysBySegment != null) {
         log.tracef("Keys by segment are: " + keysBySegment);
      }
      cache.putAll(values);

      return values;
   }

   private ClusterPublisherManager<Integer, String> cpm(Cache<Integer, String> cache) {
      return TestingUtil.extractComponent(cache, ClusterPublisherManager.class);
   }

   @DataProvider(name = "GuaranteeParallelEntry")
   public Object[][] collectionAndVersionsProvider() {
      return Arrays.stream(DeliveryGuarantee.values())
            .flatMap(dg -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                  .flatMap(parallel -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(entry -> new Object[]{dg, parallel, entry})))
            .toArray(Object[][]::new);
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCount(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache).size();

      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, null, null, null, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      } else {
         stageCount = cpm.keyReduction(parallel, null, null, null, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      assertEquals(insertAmount, actualCount.intValue());
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCountSegments(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache).size();

      IntSet targetSegments = IntSets.mutableEmptySet();

      for (int i = 2; i <= 8; ++i) {
         targetSegments.set(i);
      }

      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, targetSegments, null, null, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      } else {
         stageCount = cpm.keyReduction(parallel, targetSegments, null, null, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      int expected = findHowManyInSegments(insertAmount, targetSegments, TestingUtil.extractComponent(cache, KeyPartitioner.class));
      assertEquals(expected, actualCount.intValue());
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCountSpecificKeys(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache).size();

      Set<Integer> keysToInclude = new HashSet<>();
      for (int i = 0; i < insertAmount; i += 2) {
         keysToInclude.add(i);
      }

      // This one won't work as it isn't in the cache
      keysToInclude.add(insertAmount + 1);

      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, null, keysToInclude, null, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      } else {
         stageCount = cpm.keyReduction(parallel, null, keysToInclude, null, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      // We added all the even ones to the keysToInclude set
      int expected = insertAmount / 2;
      assertEquals(expected, actualCount.intValue());
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCountWithContext(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache).size();

      InvocationContext ctx = new NonTxInvocationContext(null);

      // These elements are removed or null - so aren't counted
      ctx.putLookedUpEntry(0, NullCacheEntry.getInstance());
      ctx.putLookedUpEntry(insertAmount - 2, Mockito.when(Mockito.mock(CacheEntry.class).isRemoved()).thenReturn(true).getMock());

      // This is an extra entry only in this context
      ctx.putLookedUpEntry(insertAmount + 1, new ImmortalCacheEntry(insertAmount + 1, insertAmount + 1));


      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, null, null, ctx, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      } else {
         stageCount = cpm.keyReduction(parallel, null, null, ctx, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      // We exclude 2 keys and add 1
      int expected = insertAmount - 1;
      assertEquals(expected, actualCount.intValue());
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCountWithContextSegments(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache).size();

      IntSet targetSegments = IntSets.mutableEmptySet();

      for (int i = 2; i <= 8; ++i) {
         targetSegments.set(i);
      }

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);

      // We only expect a subset based on the provided segments
      int expected = findHowManyInSegments(insertAmount, targetSegments, keyPartitioner);

      AtomicInteger contextChange = new AtomicInteger();

      InvocationContext ctx = new NonTxInvocationContext(null);

      // These elements are removed or null - so aren't counted
      ctx.putLookedUpEntry(0, NullCacheEntry.getInstance());
      ctx.putLookedUpEntry(insertAmount - 2, Mockito.when(Mockito.mock(CacheEntry.class).isRemoved()).thenReturn(true).getMock());

      // This is an extra entry only in this context
      ctx.putLookedUpEntry(insertAmount + 1, new ImmortalCacheEntry(insertAmount + 1, insertAmount + 1));

      // For every entry that is in the context, we only count values that are in the segments that were provided
      ctx.forEachEntry((o, e) -> {
         if (targetSegments.contains(keyPartitioner.getSegment(o))) {
            contextChange.addAndGet((e.isRemoved() || e.isNull()) ? -1 : 1);
         }
      });

      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, targetSegments, null, ctx, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      } else {
         stageCount = cpm.keyReduction(parallel, targetSegments, null, ctx, false, deliveryGuarantee,
               PublisherReducers.count(), PublisherReducers.add());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      // Two keys are removed and another is new
      assertEquals(expected + contextChange.get(), actualCount.intValue());
   }

   static int findHowManyInSegments(int insertAmount, IntSet targetSegments, KeyPartitioner kp) {
      int count = 0;
      for (int i = 0; i < insertAmount; ++i) {
         int segment = kp.getSegment(i);
         if (targetSegments.contains(segment)) {
            count++;
         }
      }
      return count;
   }

   @AfterMethod
   public void verifyNoDanglingRequests() {
      for (Cache cache : caches()) {
         // The publishers are closed asynchronously from processing of results
         eventuallyEquals(0, () -> TestingUtil.extractComponent(cache, PublisherHandler.class).openPublishers());
      }
   }

   @DataProvider(name = "GuaranteeEntry")
   public Object[][] guaranteesEntryType() {
      return Arrays.stream(DeliveryGuarantee.values())
            .flatMap(dg -> Stream.of(Boolean.TRUE, Boolean.FALSE)
                        .map(entry -> new Object[]{dg, entry}))
            .toArray(Object[][]::new);
   }

   private <I> void performPublisherOperation(DeliveryGuarantee deliveryGuarantee, boolean isEntry, IntSet segments,
         Set<Integer> keys, InvocationContext context, Map<Integer, String> expectedValues) {
      ClusterPublisherManager<Integer, String> cpm = cpm(cache(0));
      SegmentCompletionPublisher<?> publisher;
      Consumer<Object> assertConsumer;
      if (isEntry) {
         publisher = cpm.entryPublisher(segments, keys, context, false,
               deliveryGuarantee, 10, MarshallableFunctions.identity());
         assertConsumer = obj -> {
            Map.Entry<Object, Object> entry = (Map.Entry) obj;
            Object value = expectedValues.get(entry.getKey());
            assertEquals(value, entry.getValue());
         };
      } else {
         publisher = cpm.keyPublisher(segments, keys, context, false,
               deliveryGuarantee, 10, MarshallableFunctions.identity());
         assertConsumer = obj -> assertTrue(expectedValues.containsKey(obj));
      }

      int expectedSize = expectedValues.size();
      List<?> results = Flowable.fromPublisher(publisher).toList(expectedSize).blockingGet();
      if (expectedSize != results.size()) {
         log.fatal("SIZE MISMATCH expected: " + expectedValues + " was: " + results);
      }
      assertEquals(expectedSize, results.size());

      results.forEach(assertConsumer);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testSimpleIteration(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> values = insert(cache);

      performPublisherOperation(deliveryGuarantee, isEntry, null, null, null, values);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testIterationSegments(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> values = insert(cache);

      IntSet targetSegments = IntSets.mutableEmptySet();

      for (int i = 2; i <= 7; ++i) {
         targetSegments.set(i);
      }

      removeEntriesNotInSegment(values, TestingUtil.extractComponent(cache, KeyPartitioner.class), targetSegments);

      performPublisherOperation(deliveryGuarantee, isEntry, targetSegments, null, null, values);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testContextIteration(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> values = insert(cache);

      InvocationContext ctx = new NonTxInvocationContext(null);

      // These elements are removed or null - so aren't counted
      ctx.putLookedUpEntry(0, NullCacheEntry.getInstance());
      values.remove(0);
      ctx.putLookedUpEntry(7, Mockito.when(Mockito.mock(CacheEntry.class).isRemoved()).thenReturn(true).getMock());
      values.remove(7);

      // This is an extra entry only in this context
      ctx.putLookedUpEntry(156, new ImmortalCacheEntry(156, "value-" + 156));
      values.put(156, "value-" + 156);

      performPublisherOperation(deliveryGuarantee, isEntry, null, null, ctx, values);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testSpecificKeyIteration(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      Map<Integer, String> values = insert(cache);

      Set<Integer> keysToUse = new HashSet<>();
      keysToUse.add(1);
      keysToUse.add(4);
      keysToUse.add(7);
      keysToUse.add(123);

      values.entrySet().removeIf(e -> !keysToUse.contains(e.getKey()));

      performPublisherOperation(deliveryGuarantee, isEntry, null, keysToUse, null, values);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testMapIteration(DeliveryGuarantee deliveryGuarantee, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      ClusterPublisherManager<Integer, String> cpm = cpm(cache(0));
      Map<Integer, String> values = insert(cache);

      List<String> mappedValues;
      SegmentCompletionPublisher<String> publisher;
      if (isEntry) {
         mappedValues = values.entrySet().stream().map(Map.Entry::getValue).map(String::valueOf).collect(Collectors.toList());
         publisher = cpm.entryPublisher(null, null, null, false,
               deliveryGuarantee, 10,
               (SerializableFunction<Publisher<CacheEntry<Integer, String>>, Publisher<String>>) entryPublisher ->
                     Flowable.fromPublisher(entryPublisher).map(Map.Entry::getValue).map(String::valueOf));
      } else {
         mappedValues = values.keySet().stream().map(String::valueOf).collect(Collectors.toList());
         publisher = cpm.keyPublisher(null, null, null, false,
               deliveryGuarantee, 10,
               (SerializableFunction<Publisher<Integer>, Publisher<String>>) entryPublisher ->
                     Flowable.fromPublisher(entryPublisher).map(String::valueOf));
      }
      performFunctionPublisherOperation(publisher, mappedValues);
   }

   @Test(dataProvider = "GuaranteeEntry")
   public void testEmptySegmentNotification(DeliveryGuarantee deliveryGuarantee, boolean isEntry) throws InterruptedException {
      performSegmentPublisherOperation(deliveryGuarantee, isEntry, null, null, null, null);
   }

   private <I, R> void performSegmentPublisherOperation(DeliveryGuarantee deliveryGuarantee, boolean isEntry, IntSet segments,
         Set<Integer> keys, InvocationContext context, Map<Integer, String> expectedValues) throws InterruptedException {
      ClusterPublisherManager<Integer, String> cpm = cpm(cache(0));
      SegmentCompletionPublisher<R> publisher;
      if (isEntry) {
         publisher = (SegmentCompletionPublisher) cpm.entryPublisher(segments, keys, context, false,
               deliveryGuarantee, 10, MarshallableFunctions.identity());
      } else {
         publisher = (SegmentCompletionPublisher) cpm.keyPublisher(segments, keys, context, false,
               deliveryGuarantee, 10, MarshallableFunctions.identity());
      }

      IntSet mutableIntSet = IntSets.concurrentSet(10);
      TestSubscriber<R> testSubscriber = TestSubscriber.create();
      publisher.subscribe(testSubscriber, mutableIntSet::set);

      testSubscriber.await(10, TimeUnit.SECONDS);

      assertEquals(IntSets.immutableRangeSet(10), mutableIntSet);
   }

   private <I, R> void performFunctionPublisherOperation(Publisher<R> publisher, Collection<R> expectedValues) {
      int expectedSize = expectedValues.size();
      List<R> results = Flowable.fromPublisher(publisher).toList(expectedSize).blockingGet();
      if (expectedSize != results.size()) {
         log.fatal("SIZE MISMATCH was: " + results.size());
      }
      assertEquals(expectedSize, results.size());

      results.forEach(result ->
         assertTrue(expectedValues.contains(result)));
   }

   private void removeEntriesNotInSegment(Map<?, ?> map, KeyPartitioner kp, IntSet segments) {
      for (Iterator<?> keyIter = map.keySet().iterator(); keyIter.hasNext(); ) {
         Object key = keyIter.next();
         int segment = kp.getSegment(key);
         if (!segments.contains(segment)) {
            keyIter.remove();
         }
      }
   }
}
