package org.infinispan.loaders.remote;

import org.infinispan.Cache;
import org.infinispan.loaders.CacheLoader;
import org.infinispan.manager.CacheManager;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;

import static junit.framework.Assert.assertEquals;

/**
 * Simple test to sample how remote cache store is configured.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "loaders.remote.RemoteCacheStoreConfigTest", groups = "functional")
public class RemoteCacheStoreConfigTest {

   public static final String CACHE_LOADER_CONFIG = "remote-cl-config.xml";
   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotRodServer;

   @BeforeTest
   public void startUp() {
      cacheManager = TestCacheManagerFactory.createLocalCacheManager();
      assertEquals(cacheManager.getCache().size(), 0);
      hotRodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 19711);
   }

   public void simpleTest() throws IOException {
      CacheManager cm = new DefaultCacheManager(CACHE_LOADER_CONFIG);
      Cache<Object, Object> cache = cm.getCache();
      CacheLoader cacheLoader = TestingUtil.getCacheLoader(cache);
      assert cacheLoader != null;
      assert cacheLoader instanceof RemoteCacheStore;


      cache.put("k", "v");

      assertEquals(1, cacheManager.getCache().size());
      cache.stop();
      assertEquals(1, cacheManager.getCache().size());
      cm.stop();
      cm = new DefaultCacheManager(CACHE_LOADER_CONFIG);
      cache = cm.getCache();
      assertEquals("v", cache.get("k"));
   }

   @AfterTest
   public void tearDown() {
      hotRodServer.stop();
      cacheManager.stop();
   }
}
