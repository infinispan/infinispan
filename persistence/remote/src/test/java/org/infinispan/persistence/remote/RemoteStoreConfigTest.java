package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Simple test to sample how remote cache store is configured.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "persistence.remote.RemoteStoreConfigTest", groups = "functional")
public class RemoteStoreConfigTest extends AbstractInfinispanTest {

   private static final int PORT = 19711;
   public static final String CACHE_LOADER_CONFIG = "remote-cl-config.xml";
   public static final String STORE_CACHE_NAME = "RemoteStoreConfigTest";
   protected EmbeddedCacheManager cacheManager;
   private HotRodServer hotRodServer;
   protected final String cacheLoaderConfig;
   protected final String storeCacheName;
   protected final int port;

   public RemoteStoreConfigTest(String cacheLoaderConfig, String storeCacheName, int port) {
      super();
      this.cacheLoaderConfig = cacheLoaderConfig;
      this.storeCacheName = storeCacheName;
      this.port = port;
   }

   public RemoteStoreConfigTest() {
      this(CACHE_LOADER_CONFIG, STORE_CACHE_NAME, PORT);
   }

   @BeforeClass
   public void startUp() {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      Cache<?, ?> storeCache = cacheManager.createCache(this.storeCacheName, hotRodCacheConfiguration().build());
      assertEquals(0, storeCache.size());
      hotRodServer = HotRodTestingUtil.startHotRodServer(cacheManager, this.port);
   }

   public void simpleTest() throws Exception {

      String cacheName = this.storeCacheName;

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(this.cacheLoaderConfig)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache(cacheName);

            cache.put("k", "v");

            Cache<Object, Object> storeCache = cacheManager.getCache(cacheName);
            assertEquals(1, storeCache.size());
            cache.stop();
            assertEquals(1, storeCache.size());
         }
      });

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(this.cacheLoaderConfig)) {
         @Override
         public void call() {
            Cache cache = cm.getCache(cacheName);
            assertEquals("v", cache.get("k"));
         }
      });
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hotRodServer);
      hotRodServer = null;
      TestingUtil.killCacheManagers(cacheManager);
      cacheManager = null;
   }
}
