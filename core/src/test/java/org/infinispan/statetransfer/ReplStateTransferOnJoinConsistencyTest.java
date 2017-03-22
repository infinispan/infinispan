package org.infinispan.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * Test for ISPN-2362 and ISPN-2502 in replicated mode. Uses a cluster which initially has 2 nodes
 * and then a third is added to test consistency of state transfer.
 * Tests several operations both in an optimistic tx cluster (with write-skew check enabled) and in a pessimistic tx one.
 *
 * @author anistor@redhat.com
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplStateTransferOnJoinConsistencyTest")
@CleanupAfterMethod
public class ReplStateTransferOnJoinConsistencyTest extends DistStateTransferOnJoinConsistencyTest {

   @Override
   protected ConfigurationBuilder createConfigurationBuilder(boolean isOptimistic) {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true, true);
      builder.transaction().transactionMode(TransactionMode.TRANSACTIONAL)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup());

      if (isOptimistic) {
         builder.transaction().lockingMode(LockingMode.OPTIMISTIC)
               .locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      } else {
         builder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      }

      builder.clustering().l1().disable().locking().lockAcquisitionTimeout(TestingUtil.shortTimeoutMillis());
      builder.clustering().hash().numSegments(10)
            .stateTransfer().fetchInMemoryState(true).awaitInitialTransfer(false);
      return builder;
   }
}
