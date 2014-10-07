package org.infinispan.iteration;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Eviction test for entry retrieval when using a LOCAL cache to verify it works properly
 *
 * @author wburns
 * @since 7.0
 */
@Test(groups = "functional", testName = "iteration.LocalEntryRetrieverEvictionTest")
public class LocalEntryRetrieverEvictionTest extends BaseEntryRetrieverEvictionTest {
   public LocalEntryRetrieverEvictionTest() {
      super(false, CacheMode.LOCAL);
   }
}
