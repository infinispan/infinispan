package org.infinispan.persistence.rest.configuration;

import static org.infinispan.test.TestingUtil.withCacheManager;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertSame;

import org.infinispan.Cache;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.rest.RestStore;
import org.infinispan.persistence.spi.CacheLoader;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.CacheManagerCallable;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Simple test to sample how the rest cache store is configured.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Test(testName = "persistence.remote.RestCacheStoreConfigTest", groups = "functional")
public class RestCacheStoreConfigTest extends AbstractInfinispanTest {

   public static final String CACHE_LOADER_CONFIG = "rest-cl-config.xml";
   private EmbeddedCacheManager cacheManager;
   private RestServer restServer;

   @BeforeClass
   public void startUp() {
      cacheManager = TestCacheManagerFactory.createServerModeCacheManager();
      assertEquals(cacheManager.getCache().size(), 0);
      RestServerConfigurationBuilder restServerConfigurationBuilder = new RestServerConfigurationBuilder();
      restServerConfigurationBuilder.port(18212);
      restServer = new RestServer();
      restServer.start(restServerConfigurationBuilder.build(), cacheManager);
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

   @AfterClass
   public void tearDown() {
      restServer.stop();
      TestingUtil.killCacheManagers(cacheManager);
   }
}
