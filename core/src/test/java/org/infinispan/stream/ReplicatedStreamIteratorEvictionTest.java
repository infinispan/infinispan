package org.infinispan.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Eviction test for stream when using a replicated cache to verify it works properly
 *
 * @author wburns
 * @since 8.0
 */
@Test(groups = "functional", testName = "stream.ReplicatedStreamIteratorEvictionTest")
public class ReplicatedStreamIteratorEvictionTest extends BaseStreamIteratorEvictionTest {
   public ReplicatedStreamIteratorEvictionTest() {
      super(false, CacheMode.REPL_SYNC);
   }
}
