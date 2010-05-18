package org.infinispan.client.hotrod;

import org.infinispan.manager.CacheManager;
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

   CacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   private String prevValue;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      prevValue = System.setProperty(RemoteCacheManager.OVERRIDE_HOTROD_SERVERS, "localhost:" + hotrodServer.getPort());
      return cacheManager;
   }

   @AfterTest(alwaysRun = true)
   public void release() {
      if (cacheManager != null) cacheManager.stop();
      if (hotrodServer != null) hotrodServer.stop();
      if (prevValue != null) {
         System.setProperty(RemoteCacheManager.OVERRIDE_HOTROD_SERVERS, prevValue);
      } else {
         System.getProperties().remove(RemoteCacheManager.OVERRIDE_HOTROD_SERVERS);
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

   public void testUrlConstructor() throws Exception {
      URL resource = Thread.currentThread().getContextClassLoader().getResource("empty-config.properties");
      assert resource != null;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(resource);
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }
   
   public void testUrlAndBooleanConstructor() throws Exception {
      URL resource = Thread.currentThread().getContextClassLoader().getResource("empty-config.properties");
      assert resource != null;
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(resource, false);
      assert !remoteCacheManager.isStarted();
      remoteCacheManager.start();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(new Properties());
      assert remoteCacheManager.isStarted();
      assertWorks(remoteCacheManager);
      remoteCacheManager.stop();
   }

   public void testPropertiesAndBooleanConstructor() {
      RemoteCacheManager remoteCacheManager = new RemoteCacheManager(new Properties(), false);
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

   private void assertWorks(RemoteCacheManager remoteCacheManager) {
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();
      cache.put("aKey", "aValue");
      assert cache.get("aKey").equals("aValue");
   }
}
