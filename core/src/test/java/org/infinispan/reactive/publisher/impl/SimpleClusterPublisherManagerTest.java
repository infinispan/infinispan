package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.mockito.Mockito;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

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

   private int insert(Cache<Integer, String> cache) {
      int amount = 14;
      IntStream.range(0, amount).forEach(i -> cache.put(i, "value-" + i));
      return amount;
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
      int insertAmount = insert(cache);

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
      int insertAmount = insert(cache);

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
      int insertAmount = insert(cache);

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
      int insertAmount = insert(cache);

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
      int insertAmount = insert(cache);

      IntSet targetSegments = IntSets.mutableEmptySet();

      for (int i = 2; i <= 8; ++i) {
         targetSegments.set(i);
      }

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);

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
      assertEquals(insertAmount + contextChange.get(), actualCount.intValue());
   }

   private int findHowManyInSegments(int insertAmount, IntSet targetSegments, KeyPartitioner kp) {
      int count = 0;
      for (int i = 0; i < insertAmount; ++i) {
         int segment = kp.getSegment(i);
         if (targetSegments.contains(segment)) {
            count++;
         }
      }
      return count;
   }
}
