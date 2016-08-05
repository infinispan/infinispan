package org.infinispan.scattered.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stream.DistributedStreamIteratorExceptionTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public class ScatteredStreamIteratorExceptionTest extends DistributedStreamIteratorExceptionTest {
   public ScatteredStreamIteratorExceptionTest() {
      super(CacheMode.SCATTERED_SYNC);
   }
}
