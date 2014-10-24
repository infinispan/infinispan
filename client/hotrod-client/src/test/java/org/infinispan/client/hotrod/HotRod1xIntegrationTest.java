package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.util.concurrent.NotifyingFuture;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHotRodEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.k;
import static org.infinispan.test.TestingUtil.v;
import static org.testng.AssertJUnit.*;

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
