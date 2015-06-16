package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "client.hotrod.near.InvalidatedNearCacheTest")
public class InvalidatedNearCacheTest extends LazyNearCacheTest {

   @Override
   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.INVALIDATED;
   }

}
