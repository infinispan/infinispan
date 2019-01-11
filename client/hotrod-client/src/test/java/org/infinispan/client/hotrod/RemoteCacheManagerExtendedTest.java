package org.infinispan.client.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.testng.AssertJUnit.assertTrue;

import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
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
 *
 * Adds tests for remote cache mangaer which are not supported
 * by native clients (C++ and C#). See HRCPP-189 HRCPP-190.
 */
@Test(testName = "client.hotrod.RemoteCacheManagerExtendedTest", groups = "functional" )
public class RemoteCacheManagerExtendedTest extends SingleCacheManagerTest {

   HotRodServer hotrodServer;
   int port;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
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

   public void testGetUndefinedCache() {
      ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(port);
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build(), false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assert null == remoteCacheManager.getCache("Undefined1234");
   }

   public void testMarshallerInstance() {
      ConfigurationBuilder builder = HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(port);
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      builder.marshaller(marshaller);
      RemoteCacheManager newRemoteCacheManager = new RemoteCacheManager(builder.build());
      assertTrue(marshaller == newRemoteCacheManager.getMarshaller());
      HotRodClientTestingUtil.killRemoteCacheManager(newRemoteCacheManager);
   }
}
