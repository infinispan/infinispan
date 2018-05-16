package org.infinispan.persistence.rest;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.rest.RestServer;
import org.infinispan.rest.configuration.RestServerConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * @author Tristan Tarrant
 * @since 6.0
 */
@Test(testName = "persistence.remote.RestCacheStoreFunctionalTest", groups = "functional")
public class RestCacheStoreFunctionalTest extends BaseStoreFunctionalTest {
   private EmbeddedCacheManager localCacheManager;
   private RestServer restServer;

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      localCacheManager = TestCacheManagerFactory.createServerModeCacheManager();
      RestServerConfigurationBuilder restServerConfigurationBuilder = new RestServerConfigurationBuilder();
      restServerConfigurationBuilder.port(0);
      restServer = new RestServer();
      restServer.start(restServerConfigurationBuilder.build(), cacheManager);

      loaders.addStore(RestStoreConfigurationBuilder.class)
            .host("localhost")
            .port(restServer.getPort())
            .path("/rest/"+BasicCacheContainer.DEFAULT_CACHE_NAME)
            .preload(preload);
      return loaders;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() {
      return TestCacheManagerFactory.createServerModeCacheManager();
   }

   @Override
   protected void teardown() {
      super.teardown();
      if (restServer != null) {
         restServer.stop();
      }
      if (localCacheManager != null) {
         TestingUtil.killCacheManagers(localCacheManager);
      }
   }

   @Override
   public void testPreloadAndExpiry() {
      // No-op, since remote cache store does not support preload
   }

   @Override
   public void testPreloadStoredAsBinary() {
      // No-op, remote cache store does not support store as binary
      // since Hot Rod already stores them as binary
   }

   @Override
   public void testTwoCachesSameCacheStore() {
      //not applicable
   }
}
