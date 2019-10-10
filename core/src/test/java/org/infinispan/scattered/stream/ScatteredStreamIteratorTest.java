package org.infinispan.scattered.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stream.DistributedStreamIteratorTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = {"functional", "smoke"}, testName = "iteration.ScatteredStreamIteratorTest")
public class ScatteredStreamIteratorTest extends DistributedStreamIteratorTest {
   public ScatteredStreamIteratorTest() {
      super(false, CacheMode.SCATTERED_SYNC);
   }

   @Override
   public void testNodeLeavesWhileIteratingOverContainerCausingRehashToLoseValues() {
      // Test is ignored until https://issues.jboss.org/browse/ISPN-10864 can be fixed
   }

   @Override
   public void waitUntilProcessingResults() {
      // Test is ignored until https://issues.jboss.org/browse/ISPN-10864 can be fixed
   }
}
