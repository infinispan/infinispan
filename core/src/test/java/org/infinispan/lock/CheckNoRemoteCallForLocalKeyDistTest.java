package org.infinispan.lock;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus
 * @since 5.1
 */
@Test (groups = "functional", testName = "lock.CheckNoRemoteCallForLocalKeyDistTest")
public class CheckNoRemoteCallForLocalKeyDistTest extends CheckNoRemoteCallForLocalKeyTest {

   public CheckNoRemoteCallForLocalKeyDistTest() {
      mode = CacheMode.DIST_SYNC;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      super.createCacheManagers();
      key = getKeyForCache(0);
   }
}
