package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.RemoteCacheManagerTest", groups = "functional" )
public class RemoteCacheManagerTest extends SingleCacheManagerTest {

   HotRodServer hotrodServer;
   int port;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(
            hotRodCacheConfiguration());
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      port = hotrodServer.getPort();
      remoteCacheManager = null;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      TestingUtil.killCacheManagers(cacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
   }

   public void testStartStopAsync() throws Exception {
      remoteCacheManager = new RemoteCacheManager(false);

      remoteCacheManager.startAsync().get();
      assertTrue(remoteCacheManager.isStarted());

      remoteCacheManager.stopAsync().get();
      assertFalse(remoteCacheManager.isStarted());
   }
   public void testNoArgConstructor() {
      remoteCacheManager = new RemoteCacheManager();
      assertTrue(remoteCacheManager.isStarted());
   }

   public void testBooleanConstructor() {
      remoteCacheManager = new RemoteCacheManager(false);
      assertFalse(remoteCacheManager.isStarted());
      remoteCacheManager.start();
   }

   public void testConfigurationConstructor() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder
         .addServer()
            .host("127.0.0.1")
            .port(port);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      assertTrue(remoteCacheManager.isStarted());
   }
}
