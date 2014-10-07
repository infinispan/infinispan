package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Eviction test for entry retrieval when using a distributed cache to verify it works properly
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.DistributedEntryRetrieverEvictionTest")
public class DistributedEntryRetrieverEvictionTest extends BaseEntryRetrieverEvictionTest {
   public DistributedEntryRetrieverEvictionTest() {
      super(false, CacheMode.DIST_SYNC);
   }
}
