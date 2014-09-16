package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify invalidation entry behavior when a loader is present
 *
 * @author afield
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.InvalidationEntryRetrieverWithLoaderTest")
public class InvalidationEntryRetrieverWithLoaderTest extends BaseEntryRetrieverWithLoaderTest {

   public InvalidationEntryRetrieverWithLoaderTest() {
      super(false, CacheMode.INVALIDATION_SYNC, "InvalidationEntryRetrieverWithLoaderTest");
   }
}
