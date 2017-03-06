package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.0
 */
public abstract class BaseCacheAPIOptimisticTest extends CacheAPITest {
   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testForEachWithLock() throws Throwable {
      super.testForEachWithLock();
   }
}
