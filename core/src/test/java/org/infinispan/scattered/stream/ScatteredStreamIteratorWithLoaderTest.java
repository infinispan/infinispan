package org.infinispan.scattered.stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.stream.BaseStreamIteratorWithLoaderTest;
import org.testng.annotations.Test;

/**
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
@Test(groups = "functional")
public class ScatteredStreamIteratorWithLoaderTest extends BaseStreamIteratorWithLoaderTest {
   public ScatteredStreamIteratorWithLoaderTest() {
      super(false, CacheMode.SCATTERED_SYNC, "ScatteredStreamIteratorWithLoaderTest");
   }
}
