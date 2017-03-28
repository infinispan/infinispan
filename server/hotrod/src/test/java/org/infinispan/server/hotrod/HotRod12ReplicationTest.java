package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHashTopologyReceived;

import java.lang.reflect.Method;
import java.util.List;

import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.testng.annotations.Test;

/**
 * Test Hot Rod protocol version 1.2 with replicated caches.
 *
 * @author Galder Zamarreño
 * @since 5.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRod12ReplicationTest")
public class HotRod12ReplicationTest extends HotRodReplicationTest {

   @Override
   protected byte protocolVersion() {
      return 12;
   }

   @Override
   protected void checkTopologyReceived(AbstractTestTopologyAwareResponse topoResp,
                                        List<HotRodServer> servers, String cacheName) {
      assertHashTopologyReceived(topoResp, servers(), cacheName(), 0, 1, currentServerTopologyId());
   }

   @Override
   public void testSize(Method m) {
      // No-op since size() is a Hot Rod 2.0 operation
   }
}
