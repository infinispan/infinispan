package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.configuration.NearCacheMode;
import org.testng.annotations.Test;

@Deprecated
@Test(groups = "functional", testName = "client.hotrod.near.EvictLazyNearCacheTest")
public class EvictLazyNearCacheTest extends EvictEagerNearCacheTest {

   @Override
   protected NearCacheMode getNearCacheMode() {
      return NearCacheMode.LAZY;
   }

   @Override
   public void testEvictAfterReachingMax() {
      assertClient.expectNoNearEvents();
      assertClient.put(1, "v1").expectNearRemove(1);
      assertClient.put(2, "v1").expectNearRemove(2);
      assertClient.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
      assertClient.get(2, "v1").expectNearGetNull(2).expectNearPutIfAbsent(2, "v1");
      assertClient.put(3, "v1").expectNearRemove(3);
      assertClient.get(3, "v1").expectNearGetNull(3).expectNearPutIfAbsent(3, "v1");
      assertClient.get(2, "v1").expectNearGetValue(2, "v1");
      assertClient.get(3, "v1").expectNearGetValue(3, "v1");
      assertClient.get(1, "v1").expectNearGetNull(1).expectNearPutIfAbsent(1, "v1");
   }

}
