package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.killCacheManagers;

import java.util.HashMap;

import org.infinispan.client.hotrod.exceptions.RemoteCacheManagerNotStartedException;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.SingleCacheManagerTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test (testName = "client.hotrod.CacheManagerStoppedTest", groups = "functional")
public class CacheManagerStoppedTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "someName";

   EmbeddedCacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration().build());
      cacheManager.getCache(CACHE_NAME);
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build(), true);
      return cacheManager;
   }

   @AfterClass(alwaysRun = true)
   public void release() {
      killRemoteCacheManager(remoteCacheManager);
      killCacheManagers(cacheManager);
      killServers(hotrodServer);
   }

   public void testGetCacheOperations() {
      assert remoteCacheManager.getCache() != null;
      assert remoteCacheManager.getCache(CACHE_NAME) != null;
      remoteCache().put("k", "v");
      assert remoteCache().get("k").equals("v");
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
      remoteCache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAsync() {
      remoteCache().putAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testGet() {
      remoteCache().get("k");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testReplace() {
      remoteCache().replace("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testReplaceAsync() {
      remoteCache().replaceAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAll() {
      remoteCache().putAll(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testPutAllAsync() {
      remoteCache().putAllAsync(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedGet() {
      remoteCache().getVersioned("key");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedRemove() {
      remoteCache().removeWithVersion("key", 12312321l);
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class, dependsOnMethods = "testStopCacheManager")
   public void testVersionedRemoveAsync() {
      remoteCache().removeWithVersionAsync("key", 12312321l);
   }

   private RemoteCache<Object, Object> remoteCache() {
      return remoteCacheManager.getCache();
   }
}
