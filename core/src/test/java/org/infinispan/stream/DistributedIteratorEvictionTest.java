package org.infinispan.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Eviction test for stream when using a distributed cache to verify it works properly
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.DistributedIteratorEvictionTest")
public class DistributedIteratorEvictionTest extends BaseStreamIteratorEvictionTest {
   public DistributedIteratorEvictionTest() {
      super(false, CacheMode.DIST_SYNC);
   }
}
