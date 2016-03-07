package org.infinispan.stream.stress;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * Stress test designed to test to verify that distributed stream works properly when constant rehashes occur in
 * a replicated cache
 *
 * @author wburns
 * @since 8.2
 */
@Test(groups = "stress", testName = "stream.stress.ReplicatedStreamRehashStressTest")
public class ReplicatedStreamRehashStressTest extends DistributedStreamRehashStressTest {
   public ReplicatedStreamRehashStressTest() {
      super(CacheMode.REPL_SYNC);
   }
}
