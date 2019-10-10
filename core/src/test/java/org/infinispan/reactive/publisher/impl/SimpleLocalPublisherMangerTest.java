package org.infinispan.reactive.publisher.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.infinispan.Cache;
import org.infinispan.commons.util.IntSet;
import org.infinispan.commons.util.IntSets;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.InCacheMode;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import io.reactivex.Flowable;

@Test(groups = "functional", testName = "reactive.publisher.impl.SimpleLocalPublisherMangerTest")
@InCacheMode({CacheMode.REPL_SYNC, CacheMode.DIST_SYNC, CacheMode.SCATTERED_SYNC})
public class SimpleLocalPublisherMangerTest extends MultipleCacheManagersTest {
   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(cacheMode, false);
      // Reduce number of segments a bit
      builder.clustering().hash().numSegments(10);
      createCluster(builder, 3);
      waitForClusterToForm();
   }

   private Map<Integer, String> insert(Cache<Integer, String> cache) {
      int amount = 14;
      Map<Integer, String> values = new HashMap<>(amount);

      IntStream.range(0, amount).forEach(i -> values.put(i, "value-" + i));
      cache.putAll(values);
      return values;
   }

   private LocalPublisherManager<Integer, String> lpm(Cache<Integer, String> cache) {
      return TestingUtil.extractComponent(cache, LocalPublisherManager.class);
   }

   @DataProvider(name = "GuaranteeEntry")
   public Object[][] collectionAndVersionsProvider() {
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
      IntSet allSegments = IntSets.immutableRangeSet(10);
      SegmentAwarePublisher<?> publisher;
      Consumer<Object> assertConsumer;
      if (isEntry) {
         publisher = lpm.keyPublisher(allSegments, null, null, false,
               deliveryGuarantee, Function.identity());
         assertConsumer = obj -> assertTrue(inserted.containsKey(obj));
      } else {
         publisher = lpm.entryPublisher(allSegments, null, null, false,
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

      Set<Object> results = Flowable.fromPublisher(publisher).collectInto(new HashSet<>(), HashSet::add).blockingGet();
      assertEquals(expected, results.size());

      results.forEach(assertConsumer);
   }
}
