package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.testing.skip.StringLogAppender;
import org.testng.annotations.Test;

/**
 * Tests to ensure the LockingConfigurationBuilder validates properly.
 *
 * @author William Burns
 * @since 16.3
 */
@Test(groups = "functional", testName = "configuration.cache.LockingConfigurationBuilderTest")
public class LockingConfigurationBuilderTest {

   public void testDefaultClusteredTimeouts() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.clustering().remoteTimeout()).isEqualTo(15000);
      assertThat(config.locking().lockAcquisitionTimeout()).isEqualTo(10000);
   }

   public void testLocalCacheAllowsLowerRemoteTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.LOCAL).remoteTimeout(5000)
            .locking().lockAcquisitionTimeout(10000)
            .build();
      assertThat(config).isNotNull();
   }

   public void testInvalidationCacheAllowsLowerRemoteTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.INVALIDATION_SYNC).remoteTimeout(5000)
            .locking().lockAcquisitionTimeout(10000)
            .build();
      assertThat(config).isNotNull();
   }

   public void testClusteredCacheThrowsWhenRemoteTimeoutLessThanLockAcquisitionTimeout() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(5000)
            .locking().lockAcquisitionTimeout(10000)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000731");
   }

   public void testClusteredCacheWarnsWhenRemoteTimeoutEqualsLockAcquisitionTimeout() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         Configuration equalConfig = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(10000)
               .locking().lockAcquisitionTimeout(10000)
               .build();
         assertThat(equalConfig).isNotNull();
         assertThat(logAppender).anyMatch(line -> line.contains("ISPN000732"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testClusteredCacheAllowsZeroLockAcquisitionTimeoutWithoutWarning() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         Configuration config = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(5000)
               .locking().lockAcquisitionTimeout(0)
               .build();
         assertThat(config).isNotNull();
         assertThat(logAppender).noneMatch(line -> line.contains("ISPN000731") || line.contains("ISPN000732"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testClusteredCacheHigherRemoteTimeoutWithoutWarning() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         Configuration config = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.REPL_SYNC).remoteTimeout(20000)
               .locking().lockAcquisitionTimeout(10000)
               .build();
         assertThat(config).isNotNull();
         assertThat(config.clustering().remoteTimeout()).isEqualTo(20000);
         assertThat(config.locking().lockAcquisitionTimeout()).isEqualTo(10000);
         assertThat(logAppender).noneMatch(line -> line.contains("ISPN000731") || line.contains("ISPN000732"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testClusteredCacheThrowsWhenLockAcquisitionTimeoutInfiniteAndRemoteTimeoutFinite() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000)
            .locking().lockAcquisitionTimeout(-1)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000731");
   }

   public void testClusteredCacheAllowsInfiniteRemoteTimeoutWithFiniteLockTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1)
            .clustering().stateTransfer().timeout(-1)
            .locking().lockAcquisitionTimeout(10000)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.clustering().remoteTimeout()).isEqualTo(-1);
      assertThat(config.locking().lockAcquisitionTimeout()).isEqualTo(10000);
   }

   public void testClusteredCacheAllowsBothTimeoutsInfinite() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1)
            .clustering().stateTransfer().timeout(-1)
            .locking().lockAcquisitionTimeout(-1)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.clustering().remoteTimeout()).isEqualTo(-1);
      assertThat(config.locking().lockAcquisitionTimeout()).isEqualTo(-1);
   }

   public void testLocalCacheAllowsInfiniteLockTimeoutWithFiniteRemoteTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.LOCAL).remoteTimeout(5000)
            .locking().lockAcquisitionTimeout(-1)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.locking().lockAcquisitionTimeout()).isEqualTo(-1);
   }
}
