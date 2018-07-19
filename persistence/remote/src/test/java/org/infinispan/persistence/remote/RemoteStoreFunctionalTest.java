package org.infinispan.persistence.remote;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.ProtocolVersion;
import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

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
      hrServer = HotRodClientTestingUtil.startHotRodServer(localCacheManager);
      persistence
         .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName(BasicCacheContainer.DEFAULT_CACHE_NAME)
            .preload(preload)
            .addServer()
               .host("localhost")
               .port(hrServer.getPort());
      return persistence;
   }

   @Override
   protected void teardown() {
      super.teardown();
      HotRodClientTestingUtil.killServers(hrServer);
      TestingUtil.killCacheManagers(localCacheManager);
   }

   @Override
   public void testTwoCachesSameCacheStore() {
      //not applicable
   }

   @Test(expectedExceptions = CacheConfigurationException.class)
   public void testSegmentedWithUnsupportedVersion() {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.persistence()
            .addStore(RemoteStoreConfigurationBuilder.class)
            .segmented(true)
            .protocolVersion(ProtocolVersion.PROTOCOL_VERSION_21);
      cb.build();
   }

}
