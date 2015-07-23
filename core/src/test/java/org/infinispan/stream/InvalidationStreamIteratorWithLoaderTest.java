package org.infinispan.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify invalidation stream behavior when a loader is present
 *
 * @author afield
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.InvalidationStreamIteratorWithLoaderTest")
public class InvalidationStreamIteratorWithLoaderTest extends BaseStreamIteratorWithLoaderTest {

   public InvalidationStreamIteratorWithLoaderTest() {
      super(false, CacheMode.INVALIDATION_SYNC, "InvalidationStreamIteratorWithLoaderTest");
   }
}
