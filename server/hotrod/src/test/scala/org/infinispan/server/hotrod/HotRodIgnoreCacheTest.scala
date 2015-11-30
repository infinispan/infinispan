package org.infinispan.server.hotrod

import org.infinispan.server.hotrod.test.HotRodTestingUtil._
import org.testng.annotations.Test

/**
 * @author gustavonalle
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRodIgnoreCacheTest")
class HotRodIgnoreCacheTest extends HotRodSingleNodeTest {

   def testIgnoreCache()  {
      client.put("k1","v1")
      assertStatus(client.get("k1"), OperationStatus.Success)

      hotRodServer.ignoreCache(cacheName)
      assertStatus(client.get("k1"),OperationStatus.ServerError)

      hotRodServer.unignore(cacheName)
      assertStatus(client.get("k1"), OperationStatus.Success)
   }
}
