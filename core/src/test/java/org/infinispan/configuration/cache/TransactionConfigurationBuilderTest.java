package org.infinispan.configuration.cache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.testing.skip.StringLogAppender;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.DataProvider;
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

   @DataProvider(name = "cacheStopTimeouts")
   public Object[][] cacheStopTimeouts() {
      return new Object[][] {
            {CacheMode.DIST_SYNC, TransactionMode.TRANSACTIONAL, 5000L, 2},
            {CacheMode.INVALIDATION_SYNC, TransactionMode.TRANSACTIONAL, 12000L, 1},
            {CacheMode.REPL_SYNC, TransactionMode.TRANSACTIONAL, 15000L, 0},
            {CacheMode.DIST_SYNC, TransactionMode.TRANSACTIONAL, 30000L, 0},
            {CacheMode.LOCAL, TransactionMode.TRANSACTIONAL, 5000L, 1},
            {CacheMode.LOCAL, TransactionMode.TRANSACTIONAL, 0L, 1},
            {CacheMode.LOCAL, TransactionMode.TRANSACTIONAL, 10000L, 0},
            {CacheMode.DIST_SYNC, TransactionMode.NON_TRANSACTIONAL, 0L, 0}
      };
   }

   @Test(dataProvider = "cacheStopTimeouts")
   public void testCacheStopTimeoutOrdering(CacheMode cacheMode, TransactionMode transactionMode,
                                            long stopTimeout, int warnings) {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG", Level.WARN,
            t -> true, PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         Configuration config = new ConfigurationBuilder()
               .clustering().cacheMode(cacheMode).remoteTimeout(15000)
               .locking().lockAcquisitionTimeout(10000)
               .transaction().transactionMode(transactionMode).cacheStopTimeout(stopTimeout)
               .build();
         assertThat(config.transaction().cacheStopTimeout()).isEqualTo(stopTimeout);
         assertThat(logAppender).filteredOn(line -> line.contains("ISPN000744")).hasSize(warnings);
      } finally {
         logAppender.uninstall();
      }
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

   public void testInvalidationTxCompletedTxTimeoutLessThanRemoteTimeoutThrows() {
      assertThatThrownBy(() -> new ConfigurationBuilder()
            .clustering().cacheMode(CacheMode.INVALIDATION_SYNC).remoteTimeout(15000)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .completedTxTimeout(10000)
            .build())
            .isInstanceOf(CacheConfigurationException.class)
            .hasMessageContaining("ISPN000737");
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
