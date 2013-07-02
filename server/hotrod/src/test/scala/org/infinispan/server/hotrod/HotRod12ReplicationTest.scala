package org.infinispan.server.hotrod

import org.testng.annotations.Test
import test.HotRodTestingUtil._
import test.AbstractTestTopologyAwareResponse

/**
 * Test Hot Rod protocol version 1.2 with replicated caches.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = Array("functional"), testName = "server.hotrod.HotRod12ReplicationTest")
class HotRod12ReplicationTest extends HotRodReplicationTest {

   override protected def protocolVersion: Byte = 12

   @Test(enabled = false)
   override protected def checkTopologyReceived(topoResp: AbstractTestTopologyAwareResponse,
           servers: List[HotRodServer], cacheName: String) {
      assertHashTopologyReceived(topoResp, servers, cacheName, 0, 1, currentServerTopologyId)
   }

}
