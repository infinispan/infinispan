package org.infinispan.stream;

import org.infinispan.CacheCollection;
import org.infinispan.CacheStream;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.StorageType;
import org.testng.annotations.Test;

/**
 * Verifies stream tests work on a local stream with off heap enabled
 */
@Test(groups = "functional", testName = "streams.LocalStreamOffHeapTest")
public class LocalStreamOffHeapTest extends LocalStreamTest {
   @Override
   protected void enhanceConfiguration(ConfigurationBuilder builder) {
      builder.memory().storageType(StorageType.OFF_HEAP);
   }

   // Test is disabled, it assumes specific keys tie to specific segments which aren't true with
   // off heap
   @Test(enabled = false)
   @Override
   public void testKeySegmentFilter() {
      super.testKeySegmentFilter();
   }
}
