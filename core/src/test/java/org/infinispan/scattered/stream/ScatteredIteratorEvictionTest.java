package org.infinispan.scattered.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stream.BaseStreamIteratorEvictionTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional", testName = "stream.ScatteredIteratorEvictionTest")
public class ScatteredIteratorEvictionTest extends BaseStreamIteratorEvictionTest {
   public ScatteredIteratorEvictionTest() {
      super(false, CacheMode.SCATTERED_SYNC);
   }
}

