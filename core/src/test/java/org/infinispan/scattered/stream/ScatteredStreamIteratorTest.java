package org.infinispan.scattered.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stream.DistributedStreamIteratorTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.DistributedStreamIteratorTest")
public class ScatteredStreamIteratorTest extends DistributedStreamIteratorTest {
   public ScatteredStreamIteratorTest() {
      super(false, CacheMode.SCATTERED_SYNC);
   }

   // TODO: waitUntilProcessingResults unstable
}
