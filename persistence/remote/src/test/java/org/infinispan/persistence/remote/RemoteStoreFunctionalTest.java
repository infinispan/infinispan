package org.infinispan.persistence.remote;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

/**
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "persistence.remote.RemoteStoreFunctionalTest", groups = "functional")
public class RemoteStoreFunctionalTest extends BaseStoreFunctionalTest {
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;



   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      hrServer = TestHelper.startHotRodServer(localCacheManager);
      persistence
         .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(BasicCacheContainer.DEFAULT_CACHE_NAME)
            .preload(preload)
            .addServer()
               .host("localhost")
               .port(hrServer.getPort());
      return persistence;
   }

   @AfterMethod(alwaysRun = true) // run even if the test failed
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
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
