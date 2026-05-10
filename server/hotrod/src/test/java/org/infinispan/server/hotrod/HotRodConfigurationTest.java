package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BiConsumer;

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
         assertEquals(26000, cfg.locking().lockAcquisitionTimeout());
         assertEquals(31000, cfg.clustering().remoteTimeout());
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState());
         assertEquals(cfg.clustering().stateTransfer().timeout(), 31000 + distSyncTimeout);
         assertTrue(cfg.persistence().stores().isEmpty());
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
