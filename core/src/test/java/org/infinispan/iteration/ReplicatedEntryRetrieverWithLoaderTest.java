package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Test to verify replicated entry behavior when a loader is present
 *
 * @author afield
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.ReplicatedEntryRetrieverWithLoaderTest")
public class ReplicatedEntryRetrieverWithLoaderTest extends BaseEntryRetrieverWithLoaderTest {

   public ReplicatedEntryRetrieverWithLoaderTest() {
      super(false, CacheMode.REPL_SYNC, "ReplicatedEntryRetrieverWithLoaderTest");
   }
}
