package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

/**
 * @author mmarkus
 * @since 4.1
 */
@Test (testName = "client.hotrod.HotRod1xIntegrationTest", groups = "functional")
public class HotRod1xIntegrationTest extends HotRodIntegrationTest {

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            new org.infinispan.client.hotrod.configuration.ConfigurationBuilder();
      clientBuilder
            .protocolVersion("1.3")
            .addServer().host("localhost").port(hotrodServer.getPort());
      return new RemoteCacheManager(clientBuilder.build());
   }

}
