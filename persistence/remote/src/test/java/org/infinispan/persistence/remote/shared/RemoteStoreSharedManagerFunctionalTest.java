package org.infinispan.persistence.remote.shared;

import static org.infinispan.server.core.test.ServerTestingUtil.findFreePort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.host;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.client.hotrod.test.HotRodClientTestingUtil;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.remote.RemoteStoreFunctionalTest;
import org.infinispan.persistence.remote.configuration.RemoteStoreConfigurationBuilder;
import org.infinispan.persistence.remote.configuration.global.RemoteContainersConfigurationBuilder;
import org.infinispan.server.core.admin.embeddedserver.EmbeddedServerAdminOperationHandler;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.Testing;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(testName = "persistence.remote.shared.RemoteStoreSharedManagerFunctionalTest", groups = "functional")
public class RemoteStoreSharedManagerFunctionalTest extends RemoteStoreFunctionalTest {

   private EmbeddedCacheManager localCacheManager;
   private HotRodServer hrServer;

   @BeforeClass
   public void setupBefore() throws Throwable {
      Testing.retryOnFailure(() -> {
         localCacheManager = TestCacheManagerFactory.createCacheManager(hotRodCacheConfiguration());
         HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
         builder.adminOperationsHandler(new EmbeddedServerAdminOperationHandler());
         hrServer = HotRodTestingUtil.startHotRodServer(localCacheManager, host(), findFreePort(), builder, false);
      }, this::tearDown);
   }

   @AfterClass
   public void tearDown() {
      HotRodClientTestingUtil.killServers(hrServer);
      localCacheManager.stop();
   }

   @Override
   protected EmbeddedCacheManager createCacheManager(boolean start, GlobalConfigurationBuilder global, ConfigurationBuilder cb) {
      RemoteContainersConfigurationBuilder rccb = global.addModule(RemoteContainersConfigurationBuilder.class);
      rccb.addRemoteContainer("shared-container")
            .uri(String.format("hotrod://localhost:%d", hrServer.getPort()));
      rccb.addRemoteContainer("alternative")
            .uri(String.format("hotrod://localhost:%d", hrServer.getPort()));
      return super.createCacheManager(start, global, cb);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
                                                                    String cacheName, boolean preload) {
      persistence
            .addStore(RemoteStoreConfigurationBuilder.class)
            .remoteCacheName("")
            .preload(preload)
            // local cache encoding is object where as server is protostream so we can't be segmented
            .segmented(false)
            .remoteCacheContainer("shared-container");
      return persistence;
   }
}
