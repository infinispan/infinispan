package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.VersioningScheme;
import org.infinispan.statetransfer.StateTransferFunctionalTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.statetransfer.ReplTotalOrderStateTransferFunctional1PcTest")
public class ReplTotalOrderStateTransferFunctional1PcTest extends StateTransferFunctionalTest {

   protected final CacheMode mode;
   protected final boolean syncCommit;
   protected final boolean writeSkew;
   protected final boolean useSynchronization;

   public ReplTotalOrderStateTransferFunctional1PcTest() {
      this("repl-to-1pc-nbst", CacheMode.REPL_SYNC, true, true, true);
   }

   public ReplTotalOrderStateTransferFunctional1PcTest(String cacheName, CacheMode mode, boolean syncCommit,
                                                       boolean writeSkew, boolean useSynchronization) {
      super(cacheName);
      this.mode = mode;
      this.syncCommit = syncCommit;
      this.writeSkew = writeSkew;
      this.useSynchronization = useSynchronization;
   }

   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(mode, true);
      configurationBuilder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER).syncCommitPhase(syncCommit)
            .useSynchronization(useSynchronization)
            .recovery().disable();
      if (writeSkew) {
         configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true);
         configurationBuilder.versioning().enable().scheme(VersioningScheme.SIMPLE);
      } else {
         configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false);
      }
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
   }


}
