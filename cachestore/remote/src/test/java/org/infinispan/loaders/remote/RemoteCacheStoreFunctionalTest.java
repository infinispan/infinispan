package org.infinispan.loaders.remote;

import org.infinispan.client.hotrod.TestHelper;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.remote.configuration.RemoteCacheStoreConfigurationBuilder;
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
@Test(testName = "loaders.remote.RemoteCacheStoreFunctionalTest", groups = "functional")
public class RemoteCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {
   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;



   @Override
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
      hrServer = TestHelper.startHotRodServer(localCacheManager);
      loaders
         .addStore(RemoteCacheStoreConfigurationBuilder.class)
            .remoteCacheName(BasicCacheContainer.DEFAULT_CACHE_NAME)
            .addServer()
               .host("localhost")
               .port(hrServer.getPort());
      return loaders;
   }

   @AfterMethod
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
