package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;

import org.testng.annotations.Test;

/**
 * @author gustavonalle
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodIgnoreCacheTest")
public class HotRodIgnoreCacheTest extends HotRodSingleNodeTest {

   public void testIgnoreCache() {
      client().put("k1", "v1");
      assertStatus(client().get("k1"), OperationStatus.Success);

      hotRodServer.getCacheIgnore().ignoreCache(cacheName);
      assertStatus(client().get("k1"), OperationStatus.ServerError);

      hotRodServer.getCacheIgnore().unignoreCache(cacheName);
      assertStatus(client().get("k1"), OperationStatus.Success);
   }
}
