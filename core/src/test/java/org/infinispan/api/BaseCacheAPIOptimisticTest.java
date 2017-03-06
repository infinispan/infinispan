package org.infinispan.api;

import org.testng.annotations.Test;

/**
 * @author wburns
 * @since 9.1
 */
public abstract class BaseCacheAPIOptimisticTest extends CacheAPITest {
   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStream() throws Throwable {
      super.testLockedStream();
   }

   @Test(expectedExceptions = UnsupportedOperationException.class)
   @Override
   public void testLockedStreamSetValue() {
      super.testLockedStreamSetValue();
   }
}
