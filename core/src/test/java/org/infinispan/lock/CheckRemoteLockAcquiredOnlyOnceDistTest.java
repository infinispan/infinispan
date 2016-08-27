package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.CheckRemoteLockAcquiredOnlyOnceDistTest")
public class CheckRemoteLockAcquiredOnlyOnceDistTest extends CheckRemoteLockAcquiredOnlyOnceTest{

   public CheckRemoteLockAcquiredOnlyOnceDistTest() {
      mode = CacheMode.DIST_SYNC;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      key = getKeyForCache(0);
   }
}
