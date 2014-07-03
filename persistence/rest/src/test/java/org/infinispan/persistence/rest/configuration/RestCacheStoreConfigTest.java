package org.infinispan.persistence.rest.configuration;

import org.infinispan.Cache;
import org.infinispan.persistence.rest.RestStore;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.rest.EmbeddedRestServer;
import org.infinispan.rest.RestTestingUtil;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import static org.junit.Assert.assertEquals;
import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertSame;

/**
 * Simple test to sample how the rest cache store is configured.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Test(testName = "persistence.remote.RestCacheStoreConfigTest", groups = "functional")
public class RestCacheStoreConfigTest {

   public static final String CACHE_LOADER_CONFIG = "rest-cl-config.xml";
   private EmbeddedCacheManager cacheManager;
   private EmbeddedRestServer restServer;

   @BeforeTest
   public void startUp() {
      cacheManager = TestCacheManagerFactory.createCacheManager();
      assertEquals(cacheManager.getCache().size(), 0);
      restServer = RestTestingUtil.startRestServer(cacheManager, 18212);
   }

   public void simpleTest() throws Exception {
      withCacheManager(new CacheManagerCallable(TestCacheManagerFactory.fromXml(CACHE_LOADER_CONFIG)) {
         @Override
         public void call() {
            Cache<Object, Object> cache = cm.getCache();
            CacheLoader cacheLoader = TestingUtil.getCacheLoader(cache);
            assertSame(RestStore.class, cacheLoader.getClass());

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
      RestTestingUtil.killServers(restServer);
      TestingUtil.killCacheManagers(cacheManager);
   }
}
