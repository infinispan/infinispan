package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.config.Configuration;
import org.infinispan.manager.CacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.Test;

import java.util.HashMap;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.CacheManagerStoppedTest", groups = "functional")
public class CacheManagerStoppedTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "someName";

   CacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected CacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      cacheManager.defineConfiguration(CACHE_NAME, new Configuration());
      hotrodServer = TestHelper.startHotRodServer(cacheManager);
      remoteCacheManager = new RemoteCacheManager("localhost:" + hotrodServer.getPort(), true);
      return cacheManager;
   }

   @AfterTest(alwaysRun = true)
   public void release() {
      if (cacheManager != null) cacheManager.stop();
      if (hotrodServer != null) hotrodServer.stop();
   }

   public void testGetCacheOperations() {
      assert remoteCacheManager.getCache() != null;
      assert remoteCacheManager.getCache(CACHE_NAME) != null;
      cache().put("k", "v");
      assert cache().get("k").equals("v");      
   }

   @Test (dependsOnMethods = "testGetCacheOperations")
   public void testStopCacheManager() {
      assert remoteCacheManager.isStarted();
      remoteCacheManager.stop();
      assert !remoteCacheManager.isStarted();
      assert remoteCacheManager.getCache() != null;
      assert remoteCacheManager.getCache(CACHE_NAME) != null;
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGetCacheOperations2() {
      remoteCacheManager.getCache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGetCacheOperations3() {
      remoteCacheManager.getCache(CACHE_NAME).put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPut() {
      cache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAsync() {
      cache().putAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGet() {
      cache().get("k");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testReplace() {
      cache().replace("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testReplaceAsync() {
      cache().replaceAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAll() {
      cache().putAll(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAllAsync() {
      cache().putAllAsync(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedGet() {
      cache().getVersioned("key");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedRemove() {
      cache().removeWithVersion("key", 12312321l);
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedRemoveAsync() {
      cache().removeWithVersionAsync("key", 12312321l);
   }

   private RemoteCache<Object, Object> cache() {
      return remoteCacheManager.getCache();
   }
}
