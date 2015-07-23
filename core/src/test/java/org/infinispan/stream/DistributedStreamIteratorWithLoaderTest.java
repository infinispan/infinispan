package org.infinispan.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify distributed stream behavior when a loader is present
 *
 * @author wburns, afield
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.DistributedStreamIteratorWithLoaderTest")
public class DistributedStreamIteratorWithLoaderTest extends BaseStreamIteratorWithLoaderTest {

   public DistributedStreamIteratorWithLoaderTest() {
      super(false, CacheMode.DIST_SYNC, "DistributedStreamIteratorWithLoaderTest");
   }
}
