package org.infinispan.stream;

import static org.mockito.Mockito.verifyZeroInteractions;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.infinispan.Cache;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.distribution.ch.KeyPartitioner;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.stream.impl.ClusterStreamManager;
import org.infinispan.stream.impl.DistributedCacheStream;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedWriteBehindStreamIteratorTest")
public class DistributedWriteBehindStreamIteratorTest extends BaseSetupStreamIteratorTest {

   public DistributedWriteBehindStreamIteratorTest() {
      this(false, CacheMode.DIST_SYNC);
   }

   public DistributedWriteBehindStreamIteratorTest(boolean tx, CacheMode mode) {
      super(tx, mode);
   }

   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      builder.persistence().addStore(DummyInMemoryStoreConfigurationBuilder.class).shared(true).storeName(getTestName()).async().enable();
   }

   @Test
   public void testStayLocalIfAllSegmentsPresentLocallyWithWriteBehindStoreWithReHash() throws Exception {
      testStayLocalIfAllSegmentsPresentLocallyWithWriteBehindStore(true);
   }

   @Test
   public void testStayLocalIfAllSegmentsPresentLocallyWithWriteBehindStoreWithoutRehash() throws Exception {
      testStayLocalIfAllSegmentsPresentLocallyWithWriteBehindStore(false);
   }

   protected void testStayLocalIfAllSegmentsPresentLocallyWithWriteBehindStore(boolean rehashAware) {
      Cache<Object, String> cache0 = cache(0, CACHE_NAME);

      ClusterStreamManager clusterStreamManager = replaceWithSpy(cache0);

      IntStream.rangeClosed(0, 499).boxed().forEach(i -> cache0.put(i, i.toString()));

      KeyPartitioner keyPartitioner = TestingUtil.extractComponent(cache0, KeyPartitioner.class);
      ConsistentHash ch = cache0.getAdvancedCache().getDistributionManager().getWriteConsistentHash();
      Set<Integer> segmentsCache0 = ch.getSegmentsForOwner(address(0));

      CacheStream<Map.Entry<Object, String>> stream = cache0.entrySet().stream();
      if (!rehashAware) stream = stream.disableRehashAware();

      Map<Object, String> entries = mapFromIterator(stream.filterKeySegments(segmentsCache0).iterator());

      Map<Integer, Set<Map.Entry<Object, String>>> entriesPerSegment = generateEntriesPerSegment(keyPartitioner, entries.entrySet());

      // We should not see keys from other segments, but there may be segments without any keys
      assertTrue(segmentsCache0.containsAll(entriesPerSegment.keySet()));
      verifyZeroInteractions(clusterStreamManager);
      assertTrue(new DistributedCacheStream(null).hasWriteBehindSharedStore(cache0.getCacheConfiguration().persistence()));
   }
}
