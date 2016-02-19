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
@Test(groups = "functional", testName = "tx.totalorder.statetransfer.DistTotalOrderStateTransferFunctional1PcTest")
public class DistTotalOrderStateTransferFunctional1PcTest extends StateTransferFunctionalTest {

   protected final CacheMode mode;
   protected final boolean syncCommit;
   protected final boolean writeSkew;

   public DistTotalOrderStateTransferFunctional1PcTest() {
      this("dist-to-1pc-nbst", CacheMode.DIST_SYNC, true, false);
   }

   public DistTotalOrderStateTransferFunctional1PcTest(String cacheName, CacheMode mode, boolean syncCommit,
                                                       boolean writeSkew) {
      super(cacheName);
      this.mode = mode;
      this.syncCommit = syncCommit;
      this.writeSkew = writeSkew;
   }

   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(mode, true);
      configurationBuilder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER).syncCommitPhase(syncCommit)
            .recovery().disable();
      if (writeSkew) {
         configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(true);
         configurationBuilder.versioning().enable().scheme(VersioningScheme.SIMPLE);
      } else {
         configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ).writeSkewCheck(false);
      }
      configurationBuilder.clustering().stateTransfer().chunkSize(20);
   }

}
