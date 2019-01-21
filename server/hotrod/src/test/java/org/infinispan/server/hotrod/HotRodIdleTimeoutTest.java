package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestErrorResponse;
import org.testng.annotations.Test;

/**
 * Tests idle timeout logic in Hot Rod.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodIdleTimeoutTest")
public class HotRodIdleTimeoutTest extends HotRodSingleNodeTest {

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      return startHotRodServer(cacheManager, serverPort(), 5);
   }

   @Override
   protected HotRodClient connectClient() {
      return new HotRodClient("127.0.0.1", server().getPort(), cacheName, 10, (byte) 20);
   }

   public void testSendPartialRequest(Method m) {
      client().assertPut(m);
      TestErrorResponse resp = client().executePartial(0xA0, (byte) 0x03, cacheName, k(m), 0, 0, v(m), 0);
      assertNull(resp); // No response received within expected timeout.
      client().assertPutFail(m);
      shutdownClient();

      HotRodClient newClient = connectClient();
      try {
         newClient.assertPut(m);
      } finally {
         killClient(newClient);
      }
   }

}
