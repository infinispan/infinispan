package org.infinispan.stats;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.impl.DefaultSegmentedDataContainer;
import org.infinispan.container.impl.InternalDataContainer;
import org.infinispan.container.impl.PeekableTouchableContainerMap;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * A cache that does not need segments has a data container with a single segment, and the approximate
 * entries statistics must only ask for that segment.
 */
@Test(groups = "functional", testName = "stats.LocalCacheApproximateEntriesTest")
public class LocalCacheApproximateEntriesTest extends SingleCacheManagerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.statistics().enable();
      return TestCacheManagerFactory.createCacheManager(builder);
   }

   public void testApproximateEntriesWithSingleSegmentContainer() {
      TestingUtil.replaceComponent(cache, InternalDataContainer.class,
            new DefaultSegmentedDataContainer<>(PeekableTouchableContainerMap::new, 1), true);

      cache.put("key1", "value1");
      cache.put("key2", "value2");

      Stats stats = cache.getAdvancedCache().getStats();
      assertEquals(2, stats.getApproximateEntries());
      assertEquals(2, stats.getApproximateEntriesUnique());
   }
}
