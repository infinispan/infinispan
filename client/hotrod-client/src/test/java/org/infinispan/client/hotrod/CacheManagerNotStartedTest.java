package org.infinispan.client.hotrod;

import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killRemoteCacheManager;
import static org.infinispan.client.hotrod.test.HotRodClientTestingUtil.killServers;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

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
@Test (testName = "client.hotrod.CacheManagerNotStartedTest", groups = "functional")
public class CacheManagerNotStartedTest extends SingleCacheManagerTest {

   private static final String CACHE_NAME = "someName";

   EmbeddedCacheManager cacheManager = null;
   HotRodServer hotrodServer = null;
   RemoteCacheManager remoteCacheManager;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      cacheManager = TestCacheManagerFactory
            .createCacheManager(hotRodCacheConfiguration());
      cacheManager.defineConfiguration(CACHE_NAME, hotRodCacheConfiguration().build());
      return cacheManager;
   }

   @Override
   protected void setup() throws Exception {
      super.setup();
      hotrodServer = HotRodClientTestingUtil.startHotRodServer(cacheManager);
      org.infinispan.client.hotrod.configuration.ConfigurationBuilder clientBuilder =
            HotRodClientTestingUtil.newRemoteConfigurationBuilder();
      clientBuilder.addServer().host("localhost").port(hotrodServer.getPort());
      remoteCacheManager = new RemoteCacheManager(clientBuilder.build(), false);
   }

   @AfterClass
   @Override
   protected void destroyAfterClass() {
      super.destroyAfterClass();
      killRemoteCacheManager(remoteCacheManager);
      killServers(hotrodServer);
   }

   public void testGetCacheOperations() {
      assert remoteCacheManager.getCache() != null;
      assert remoteCacheManager.getCache(CACHE_NAME) != null;
      assert !remoteCacheManager.isStarted();
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testGetCacheOperations2() {
      remoteCacheManager.getCache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testGetCacheOperations3() {
      remoteCacheManager.getCache(CACHE_NAME).put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testPut() {
      remoteCache().put("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testPutAsync() {
      remoteCache().putAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testGet() {
      remoteCache().get("k");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testReplace() {
      remoteCache().replace("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testReplaceAsync() {
      remoteCache().replaceAsync("k", "v");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testPutAll() {
      remoteCache().putAll(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testPutAllAsync() {
      remoteCache().putAllAsync(new HashMap<Object, Object>());
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testVersionedGet() {
      remoteCache().getVersioned("key");
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testVersionedRemove() {
      remoteCache().removeWithVersion("key", 12312321l);
   }

   @Test(expectedExceptions = RemoteCacheManagerNotStartedException.class)
   public void testVersionedRemoveAsync() {
      remoteCache().removeWithVersionAsync("key", 12312321l);
   }

   private RemoteCache<Object, Object> remoteCache() {
      return remoteCacheManager.getCache();
   }
}
