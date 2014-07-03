package org.infinispan.persistence.remote;

import org.infinispan.Cache;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.test.TestingUtil.withCacheManager;

/**
 * Simple test to sample how remote cache store is configured.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "persistence.remote.RemoteStoreConfigTest", groups = "functional")
public class RemoteStoreConfigTest {

   public static final String CACHE_LOADER_CONFIG = "remote-cl-config.xml";
   private EmbeddedCacheManager cacheManager;
   private HotRodServer hotRodServer;

   @BeforeTest
   public void startUp() {
      cacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      assertEquals(cacheManager.getCache().size(), 0);
      hotRodServer = HotRodTestingUtil.startHotRodServer(cacheManager, 19711);
   }

   public void simpleTest() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            CacheLoader cacheLoader = TestingUtil.getCacheLoader(cache);
            assert cacheLoader != null;
            assert cacheLoader instanceof RemoteStore;


            cache.put("k", "v");

            assertEquals(1, cacheManager.getCache().size());
            cache.stop();
            assertEquals(1, cacheManager.getCache().size());
         }
      });

      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
         @Override
         public void call() {
            Cache cache = cm.getCache();
            assertEquals("v", cache.get("k"));
         }
      });
   }

   @AfterTest
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hotRodServer);
      TestingUtil.killCacheManagers(cacheManager);
   }
}
