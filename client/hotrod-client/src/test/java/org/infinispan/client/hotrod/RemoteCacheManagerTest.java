package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.client.hotrod.impl.ConfigurationProperties;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.net.URL;
import java.util.Properties;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.RemoteCacheManagerTest", groups = "functional" )
public class RemoteCacheManagerTest extends SingleCacheManagerTest {

   EmbeddedCacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   int port;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      port = hotrodServer.getPort();
      return cacheManager;
   }

   @AfterTest(alwaysRun = true)
   public void release() {
      try {
         if (cacheManager != null) cacheManager.stop();
         if (hotrodServer != null) hotrodServer.stop();
      } catch (Exception e) {
         e.printStackTrace();
      }
   }

   public void testNoArgConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager();
      assert remoteCacheManager.isStarted();
      remoteCacheManager.stop();
   }

   public void testBooleanConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      remoteCacheManager.stop();
   }
   
   public void testUrlAndBooleanConstructor() throws Exception {
      URL resource = Thread.currentThread().getContextClassLoader().getResource("empty-config.properties");
      assert resource != null;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(resource, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.config.getProperties().setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesConstructor() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(p);
      assert remoteCacheManager.isStarted();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesAndBooleanConstructor() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(p, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testStringAndBooleanConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager("localhost:"+hotrodServer.getPort(), false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   @Test(expectedExceptions = HotRodClientException.class, expectedExceptionsMessageRegExp = ".*CacheNotFoundException.*")
   public void testGetUndefinedCache() {
      Properties p = new Properties();
      p.setProperty(ConfigurationProperties.SERVER_LIST, "127.0.0.1:" + port);
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(p, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache("Undefined1234");
      cache.put("aKey", "aValue");
   }

   private void assertWorks(RemoteCacheManager remoteCacheManager) {
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
      cache.put("aKey", "aValue");
      assert cache.get("aKey").equals("aValue");
   }
}
