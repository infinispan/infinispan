package org.infinispan.client.hotrod;

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

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
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
