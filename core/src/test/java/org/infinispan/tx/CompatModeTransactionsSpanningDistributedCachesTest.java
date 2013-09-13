package org.infinispan.tx;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Test basic functionality of transactional caches in compatibility mode.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "tx.CompatModeTransactionsSpanningDistributedCachesTest")
public class CompatModeTransactionsSpanningDistributedCachesTest extends TransactionsSpanningDistributedCachesTest {

   @Override
   protected ConfigurationBuilder getConfiguration() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true, false);
      builder.compatibility().enable()
            .clustering()
            .cacheMode(CacheMode.DIST_SYNC)
            .stateTransfer().fetchInMemoryState(false)
            .transaction().syncCommitPhase(true).syncRollbackPhase(true)
            .cacheStopTimeout(0L);
      return builder;
   }

}

