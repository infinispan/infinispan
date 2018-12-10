package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.reactive.publisher.PublisherReducers;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
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
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      } else {
         stageCount = cpm.keyReduction(parallel, null, null, null, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
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
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      } else {
         stageCount = cpm.keyReduction(parallel, targetSegments, null, null, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      int expected = findHowManyInSegments(insertAmount, targetSegments, cache);
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
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      } else {
         stageCount = cpm.keyReduction(parallel, null, keysToInclude, null, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      // We added all the even ones to the keysToInclude set
      int expected = insertAmount / 2;
      assertEquals(expected, actualCount.intValue());
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCountExcludedKeys(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache);

      Set<Integer> keysToExclude = new HashSet<>();
      keysToExclude.add(0);
      keysToExclude.add(insertAmount - 2);

      // This one won't work as it isn't in the cache
      keysToExclude.add(insertAmount + 1);

      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, null, null, keysToExclude, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      } else {
         stageCount = cpm.keyReduction(parallel, null, null, keysToExclude, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      // We exclude 3 keys, but only 2 are in the cache
      int expected = insertAmount - 2;
      assertEquals(expected, actualCount.intValue());
   }

   @Test(dataProvider = "GuaranteeParallelEntry")
   public void testCountExcludedKeySegments(DeliveryGuarantee deliveryGuarantee, boolean parallel, boolean isEntry) {
      Cache<Integer, String> cache = cache(0);
      int insertAmount = insert(cache);

      IntSet targetSegments = IntSets.mutableEmptySet();

      for (int i = 2; i <= 8; ++i) {
         targetSegments.set(i);
      }

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache, KeyPartitioner.class);

      boolean includedNonSegmentKey = false;

      // We create an exclusion set that has at least 3 entries, one of which is always not part of the segments
      // This means the count will have (keysToExclude - 1) less count
      Set<Integer> keysToExclude = new HashSet<>();
      for (int i = 0; i < insertAmount; ++i) {
         int segment = keyPartitioner.getSegment(i);
         if (targetSegments.contains(segment)) {
            keysToExclude.add(i);
         } else if (!includedNonSegmentKey) {
            includedNonSegmentKey = true;
            keysToExclude.add(i);
         } else if (keysToExclude.size() >= 2) {
            break;
         }
      }

      ClusterPublisherManager<Integer, String> cpm = cpm(cache);
      CompletionStage<Long> stageCount;
      if (isEntry) {
         stageCount = cpm.entryReduction(parallel, targetSegments, null, keysToExclude, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      } else {
         stageCount = cpm.keyReduction(parallel, targetSegments, null, keysToExclude, false, deliveryGuarantee,
               PublisherReducers.sumReducer(), PublisherReducers.sumFinalizer());
      }

      Long actualCount = stageCount.toCompletableFuture().join();
      // One of the excluded keys wasn't present
      int expected = findHowManyInSegments(insertAmount, targetSegments, cache) - keysToExclude.size() + 1;
      assertEquals(expected, actualCount.intValue());
   }

   private int findHowManyInSegments(int insertAmount, IntSet targetSegments, Cache<Integer, String> cache) {
      KeyPartitioner kp = TestingUtil.extractComponent(cache, KeyPartitioner.class);

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
