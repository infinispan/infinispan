package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Eviction test for entry retrieval when using a replicated cache to verify it works properly
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.ReplicatedEntryRetrieverEvictionTest")
public class ReplicatedEntryRetrieverEvictionTest extends BaseEntryRetrieverEvictionTest {
   public ReplicatedEntryRetrieverEvictionTest() {
      super(false, CacheMode.REPL_SYNC);
   }
}
