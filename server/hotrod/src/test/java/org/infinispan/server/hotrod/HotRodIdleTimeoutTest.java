package org.infinispan.server.hotrod;

import static org.infinispan.commons.test.Exceptions.expectException;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;

import java.lang.reflect.Method;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.CompletionException;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.testng.annotations.Test;

/**
 * Tests idle timeout logic in Hot Rod.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodIdleTimeoutTest")
public class HotRodIdleTimeoutTest extends HotRodSingleNodeTest {
   public static final int IDLE_TIMEOUT = 1;

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      return startHotRodServer(cacheManager, serverPort(), IDLE_TIMEOUT);
   }

   @Override
   protected HotRodClient connectClient() {
      return new HotRodClient("127.0.0.1", server().getPort(), cacheName, (byte) 20);
   }

   public void testSendPartialRequest(Method m) {
      client().assertPut(m);
      expectException(CompletionException.class, ClosedChannelException.class,
                      () -> client().executePartial(0xA0, (byte) 0x03, cacheName, k(m), 0, 0, v(m), 0));

      // The connection cannot be used to send another request
      client().assertPutFail(m);
      shutdownClient();

      // But another connection will work
      HotRodClient newClient = connectClient();
      try {
         newClient.assertPut(m);
      } finally {
         killClient(newClient);
      }
   }

}
