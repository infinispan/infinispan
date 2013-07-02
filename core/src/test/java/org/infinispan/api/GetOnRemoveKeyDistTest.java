package org.infinispan.api;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

@Test (groups = "functional", testName = "api.GetOnRemoveKeyDistTest")
public class GetOnRemoveKeyDistTest extends GetOnRemovedKeyTest {

   public GetOnRemoveKeyDistTest() {
      mode = CacheMode.DIST_SYNC;
   }

   @Override
   protected Object getKey() {
      return getKeyForCache(0);
   }
}
