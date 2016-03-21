package org.infinispan.client.hotrod;

import org.testng.annotations.Test;

import java.util.Properties;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;

/**
 * @author mmarkus
 * @since 4.1
 */
@Test (testName = "client.hotrod.HotRod1xIntegrationTest", groups = "functional")
public class HotRod1xIntegrationTest extends HotRodIntegrationTest {

   @Override
   protected RemoteCacheManager getRemoteCacheManager() {
      Properties config = new Properties();
      config.put("infinispan.client.hotrod.server_list", "127.0.0.1:" + hotrodServer.getPort());
      config.put("infinispan.client.hotrod.protocol_version", "1.3");
      return new RemoteCacheManager(config);
   }

}
