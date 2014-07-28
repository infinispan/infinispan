package org.infinispan.persistence.rest;

import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.rest.configuration.RestStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.rest.EmbeddedRestServer;
import org.infinispan.rest.RestTestingUtil;
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
   private EmbeddedRestServer restServer;

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      localCacheManager = TestCacheManagerFactory.createCacheManager();
      restServer = RestTestingUtil.startRestServer(localCacheManager);
      loaders.addStore(RestStoreConfigurationBuilder.class)
            .host("localhost")
            .port(restServer.getPort())
            .path("/rest/"+BasicCacheContainer.DEFAULT_CACHE_NAME)
            .preload(preload);
      return loaders;
   }

   @Override
   protected void teardown() {
      super.teardown();
      if (restServer != null) {
         RestTestingUtil.killServers(restServer);
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
