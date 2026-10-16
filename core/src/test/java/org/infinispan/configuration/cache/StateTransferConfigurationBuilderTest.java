package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.testing.skip.StringLogAppender;
import org.testng.annotations.Test;

/**
 * Tests to ensure StateTransferConfigurationBuilder and related transport timeouts validate properly.
 *
 * @author William Burns
 * @since 16.3
 */
@Test(groups = "functional", testName = "configuration.cache.StateTransferConfigurationBuilderTest")
public class StateTransferConfigurationBuilderTest {

   public void testDefaultStateTransferTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.clustering().stateTransfer().timeout()).isEqualTo(240000);
      assertThat(config.clustering().remoteTimeout()).isEqualTo(15000);
   }

   public void testStateTransferTimeoutLessThanRemoteTimeoutThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000)
            .stateTransfer().timeout(10000)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000636");
   }

   public void testStateTransferTimeoutEqualsRemoteTimeoutWarns() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         Configuration config = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000)
               .stateTransfer().timeout(15000)
               .build();
         assertThat(config).isNotNull();
         assertThat(logAppender).anyMatch(line -> line.contains("ISPN000739"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testDistributedSyncTimeoutLessThanRemoteTimeoutThrows() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().transport(new JGroupsTransport()).distributedSyncTimeout(10000)
            .build();

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000);

      assertThatThrownBy(() -> cb.validate(globalConfig))
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000740");
   }

   public void testDistributedSyncTimeoutEqualsRemoteTimeoutWarns() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().transport(new JGroupsTransport()).distributedSyncTimeout(15000)
            .build();

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000);

      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         cb.validate(globalConfig);
         assertThat(logAppender).anyMatch(line -> line.contains("ISPN000741"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testLocalCacheAllowsLowerDistributedSyncTimeout() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().transport(new JGroupsTransport()).distributedSyncTimeout(10000)
            .build();

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.LOCAL).remoteTimeout(15000);

      cb.validate(globalConfig);
   }

   public void testStateTransferThrowsWhenRemoteTimeoutInfiniteAndStateTransferTimeoutFinite() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1)
            .stateTransfer().timeout(10000)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000636");
   }

   public void testStateTransferAllowsInfiniteStateTransferTimeoutWithFiniteRemoteTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000)
            .stateTransfer().timeout(-1)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.clustering().stateTransfer().timeout()).isEqualTo(-1);
   }

   public void testStateTransferAllowsBothTimeoutsInfinite() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1)
            .stateTransfer().timeout(-1)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.clustering().stateTransfer().timeout()).isEqualTo(-1);
      assertThat(config.clustering().remoteTimeout()).isEqualTo(-1);
   }

   public void testDistributedSyncTimeoutThrowsWhenRemoteTimeoutInfinite() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().transport(new JGroupsTransport()).distributedSyncTimeout(240000)
            .build();

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1);

      assertThatThrownBy(() -> cb.validate(globalConfig))
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000740");
   }

   public void testDistributedSyncTimeoutAllowsInfiniteDistributedSyncTimeoutWithFiniteRemoteTimeout() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().transport(new JGroupsTransport()).distributedSyncTimeout(-1)
            .build();

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000);

      cb.validate(globalConfig);
   }

   public void testDistributedSyncTimeoutAllowsBothTimeoutsInfinite() {
      GlobalConfiguration globalConfig = new GlobalConfigurationBuilder()
            .transport().transport(new JGroupsTransport()).distributedSyncTimeout(-1)
            .build();

      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1);

      cb.validate(globalConfig);
   }
}
