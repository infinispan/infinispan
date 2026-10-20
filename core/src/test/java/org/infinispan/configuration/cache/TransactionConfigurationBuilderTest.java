package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.testing.skip.StringLogAppender;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Tests to ensure the TransactionConfigurationBuilder validates properly.
 *
 * @author William Burns
 * @since 16.3
 */
@Test(groups = "functional", testName = "configuration.cache.TransactionConfigurationBuilderTest")
public class TransactionConfigurationBuilderTest {

   public void testDefaultTimeouts() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.transaction().completedTxTimeout()).isEqualTo(60000);
      assertThat(config.transaction().reaperWakeUpInterval()).isEqualTo(30000);
      assertThat(config.transaction().cacheStopTimeout()).isEqualTo(30000);
   }

   public void testZeroReaperWakeUpIntervalThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .transaction().reaperWakeUpInterval(0)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000344");
   }

   public void testNegativeReaperWakeUpIntervalThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .transaction().reaperWakeUpInterval(-1)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000344");
   }

   public void testZeroCompletedTxTimeoutThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .transaction().completedTxTimeout(0)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000345");
   }

   public void testNegativeCompletedTxTimeoutThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .transaction().completedTxTimeout(-1)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000345");
   }

   public void testNegativeCacheStopTimeoutThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .transaction().cacheStopTimeout(-1)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000736");
   }

   public void testZeroCacheStopTimeoutAllowed() {
      Configuration config = new ConfigurationBuilder()
            .transaction().cacheStopTimeout(0)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.transaction().cacheStopTimeout()).isEqualTo(0);
   }

   public void testClusteredTxCompletedTxTimeoutLessThanRemoteTimeoutThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .completedTxTimeout(10000)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000737");
   }

   public void testClusteredTxCompletedTxTimeoutEqualsRemoteTimeoutWarns() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         Configuration config = new ConfigurationBuilder()
               .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(15000)
               .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
               .completedTxTimeout(15000)
               .build();
         assertThat(config).isNotNull();
         assertThat(logAppender).anyMatch(line -> line.contains("ISPN000738"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testLocalTxAllowsLowerCompletedTxTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.LOCAL).remoteTimeout(15000)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .completedTxTimeout(10000)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.transaction().completedTxTimeout()).isEqualTo(10000);
   }

   public void testClusteredTxThrowsWhenRemoteTimeoutInfiniteAndCompletedTxTimeoutFinite() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.DIST_SYNC).remoteTimeout(-1)
            .clustering().stateTransfer().timeout(-1)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .completedTxTimeout(60000)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000737");
   }

   public void testLocalTxAllowsInfiniteRemoteTimeoutWithFiniteCompletedTxTimeout() {
      Configuration config = new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.LOCAL).remoteTimeout(-1)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .completedTxTimeout(10000)
            .build();
      assertThat(config).isNotNull();
      assertThat(config.transaction().completedTxTimeout()).isEqualTo(10000);
   }
}
