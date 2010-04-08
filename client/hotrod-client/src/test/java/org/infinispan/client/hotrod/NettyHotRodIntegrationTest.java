package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.netty.NettyTransportFactory;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Marku127.0.0.1:11311;127.0.0.2:11411s@jboss.com
 * @since 4.1
 */
@Test(testName = "hotrod.NettyHotRodIntegrationTest", groups = "functional", enabled = false, description = "TODO To be re-enabled when we have a multithreaded HotRod server impl")
public class NettyHotRodIntegrationTest extends HotRodIntegrationTest {
   
   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      Properties props = new Properties();
      props.put("transport-factory", NettyTransportFactory.class.getName());
      props.put("hotrod-servers", "127.0.0.1:11311;127.0.0.2:11411");
      return new RemoteCacheManager(props, true);
   }

   @Override
   public void testPut() {
      super.testPut();    // TODO: Customise this generated block
   }
}
