package org.infinispan.client.hotrod;

import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.assertFalse;
import org.infinispan.client.hotrod.configuration.ConfigurationBuilder;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.Properties;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

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

   @AfterTest
   public void release() {
      TestingUtil.killCacheManagers(cacheManager);
      HotRodClientTestingUtil.killServers(hotrodServer);
      HotRodClientTestingUtil.killRemoteCacheManager(remoteCacheManager);
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

   public void testUrlAndBooleanConstructor() throws Exception {
      URL resource = Thread.currentThread().getContextClassLoader().getResource("empty-config.properties");
      assert resource != null;
      remoteCacheManager = new RemoteCacheManager(resource, false);
      assert !remoteCacheManager.isStarted();
      Properties properties = remoteCacheManager.getProperties();
      properties.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      remoteCacheManager = new RemoteCacheManager(properties, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
   }

   public void testPropertiesConstructor() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      remoteCacheManager = new RemoteCacheManager(p);
      assert remoteCacheManager.isStarted();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesAndBooleanConstructor() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      remoteCacheManager = new RemoteCacheManager(p, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
   }

   public void testStringAndBooleanConstructor() {
      remoteCacheManager = new RemoteCacheManager("localhost:"+hotrodServer.getPort(), false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
   }

   public void testGetUndefinedCache() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      remoteCacheManager = new RemoteCacheManager(p, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assert null == remoteCacheManager.getCache("Undefined1234");
   }

   private void assertWorks(RemoteCacheManager remoteCacheManager) {
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
      cache.put("aKey", "aValue");
      assert cache.get("aKey").equals("aValue");
   }

   public void testMarshallerInstance() {
      ConfigurationBuilder builder = new ConfigurationBuilder();
      builder.addServer().host("127.0.0.1").port(port);
      GenericJBossMarshaller marshaller = new GenericJBossMarshaller();
      builder.marshaller(marshaller);
      remoteCacheManager = new RemoteCacheManager(builder.build());
      assertTrue(marshaller == remoteCacheManager.getMarshaller());
   }
}
