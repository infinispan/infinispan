package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.ServerRestartTest", groups = "functional")
public class ServerRestartTest extends SingleCacheManagerTest {

   private static final Log log = LogFactory.getLog(HotRodIntegrationTest.class);

   private RemoteCache<String, String> defaultRemote;
   private RemoteCacheManager remoteCacheManager;

   protected HotRodServer hotrodServer;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      log.info("Started server on port: " + hotrodServer.getPort());

      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(hotrodServer.getPort()).connectionPool().timeBetweenEvictionRuns(2000);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      defaultRemote = remoteCacheManager.getCache();
   }

   @AfterClass
   public void testDestroyRemoteCacheFactory() {
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
   }

   public void testServerShutdown() throws Exception {
      defaultRemote.put("k","v");
      assert defaultRemote.get("k").equals("v");

      int port = hotrodServer.getPort();
      hotrodServer.stop();

      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.name(ServerRestartTest.class.getSimpleName());
      builder.host("127.0.0.1").port(port).workerThreads(2).idleTimeout(20000).tcpNoDelay(true).sendBufSize(15000).recvBufSize(25000);
      hotrodServer.start(builder.build(), cacheManager);

      Thread.sleep(3000);

      assert defaultRemote.get("k").equals("v");
      defaultRemote.put("k","v");
   }
}
