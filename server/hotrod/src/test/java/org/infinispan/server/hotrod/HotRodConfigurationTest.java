package org.infinispan.server.hotrod;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.function.BiConsumer;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.server.core.test.Stoppable;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.skip.StringLogAppender;
import org.testng.annotations.Test;

/**
 * Test to verify that configuration changes are reflected in backend caches.
 *
 * @author Galder Zamarreño
 * @since 4.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodConfigurationTest")
public class HotRodConfigurationTest extends AbstractInfinispanTest {

   public void testDefaultTimeouts() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      withClusteredServer(builder, (cfg, distSyncTimeout) -> {
         assertEquals(10000, cfg.locking().lockAcquisitionTimeout());
         assertEquals(15000, cfg.clustering().remoteTimeout());
         assertTrue(cfg.clustering().stateTransfer().fetchInMemoryState());
         assertEquals(cfg.clustering().stateTransfer().timeout(), 15000 + distSyncTimeout);
         assertTrue(cfg.persistence().stores().isEmpty());
      });
   }

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

   public void testValidationThrowsWhenReplTimeoutLessThanLockTimeout() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyLockTimeout(20000).topologyReplTimeout(15000);
      assertThatThrownBy(builder::validate)
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN006030");
   }

   public void testValidationWarnsWhenReplTimeoutEqualsLockTimeout() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
         builder.topologyLockTimeout(15000).topologyReplTimeout(15000);
         builder.validate();
         assertThat(logAppender).anyMatch(line -> line.contains("ISPN006031"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testValidationThrowsWhenLockTimeoutInfiniteAndReplTimeoutFinite() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyLockTimeout(-1).topologyReplTimeout(15000);
      assertThatThrownBy(builder::validate)
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN006030");
   }

   public void testValidationAllowsInfiniteReplTimeoutWithFiniteLockTimeout() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyLockTimeout(10000).topologyReplTimeout(-1);
      builder.validate();
   }

   public void testValidationAllowsBothTimeoutsInfinite() {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.topologyLockTimeout(-1).topologyReplTimeout(-1);
      builder.validate();
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
