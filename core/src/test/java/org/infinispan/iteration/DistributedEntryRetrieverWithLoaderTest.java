package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify distributed entry behavior when a loader is present
 *
 * @author wburns, afield
 * @since 7.0
 */
@Test(groups = "functional", testName = "distexec.DistributedEntryRetrieverWithLoaderTest")
public class DistributedEntryRetrieverWithLoaderTest extends BaseEntryRetrieverWithLoaderTest {

   public DistributedEntryRetrieverWithLoaderTest() {
      super(false, CacheMode.DIST_SYNC, "DistributedEntryRetrieverWithLoaderTest");
   }
}
