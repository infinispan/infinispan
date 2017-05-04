package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.function.BiConsumer;

import org.infinispan.configuration.cache.ClusterLoaderConfiguration;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test to verify that configuration changes are reflected in backend caches.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodConfigurationTest")
public class HotRodConfigurationTest extends AbstractInfinispanTest {

   public void testUserDefinedTimeouts() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyLockTimeout(26000).topologyReplTimeout(31000);
      withClusteredServer(builder, (cfg, distSyncTimeout) -> {
         assertEquals(cfg.locking().lockAcquisitionTimeout(), 26000);
         assertEquals(cfg.clustering().remoteTimeout(), 31000);
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState());
         assertEquals(cfg.clustering().stateTransfer().timeout(), 31000 + distSyncTimeout);
         assertTrue(cfg.persistence().stores().isEmpty());
      });
   }

   public void testLazyLoadTopology() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyStateTransfer(false).topologyReplTimeout(43000);
      withClusteredServer(builder, (cfg, distSyncTimeout) -> {
         assertEquals(cfg.clustering().remoteTimeout(), 43000);
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState());
         ClusterLoaderConfiguration clcfg = ((ClusterLoaderConfiguration) cfg.persistence().stores().get(0));
         assertNotNull(clcfg);
         assertEquals(clcfg.remoteCallTimeout(), 43000);
      });
   }

   private void withClusteredServer(HotRodServerConfigurationBuilder builder,
                                    BiConsumer<Configuration, Long> consumer) {
      Stoppable.useCacheManager(TestCacheManagerFactory.createClusteredCacheManager(hotRodCacheConfiguration()), cm ->
            Stoppable.useServer(startHotRodServer(cm, serverPort(), builder), server -> {
               Configuration cfg = cm.getCacheConfiguration(server.getConfiguration().topologyCacheName());
               consumer.accept(cfg, cm.getCacheManagerConfiguration().transport().distributedSyncTimeout());
            }));
   }

}
