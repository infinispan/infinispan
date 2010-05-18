package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.impl.transport.netty.NettyTransportFactory;
import org.infinispan.config.Configuration;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.util.Properties;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "hotrod.NettyHotRodIntegrationTest", groups = "functional")
public class NettyHotRodIntegrationTest extends HotRodIntegrationTest {
   
   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      Properties props = new Properties();
      props.put("transport-factory", NettyTransportFactory.class.getName());
      props.put("hotrod-servers", "127.0.0.1:" + hotrodServer.getPort());
      return new RemoteCacheManager(props, true);
   }
}
