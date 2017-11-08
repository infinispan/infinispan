package org.infinispan.tx.totalorder.statetransfer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.StorageType;
import org.infinispan.statetransfer.StateTransferFunctionalTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "functional", testName = "tx.totalorder.statetransfer.TotalOrderStateTransferFunctionalTest")
public class TotalOrderStateTransferFunctionalTest extends StateTransferFunctionalTest {

   protected final boolean writeSkew;
   protected final boolean useSynchronization;
   protected final StorageType storage;

   @Override
   public Object[] factory() {
      return new Object[] {
         new TotalOrderStateTransferFunctionalTest("dist-to-1pc-nbst", CacheMode.DIST_SYNC, false, false, StorageType.OBJECT),
         new TotalOrderStateTransferFunctionalTest("dist-to-2pc-nbst", CacheMode.DIST_SYNC, true, false, StorageType.OBJECT),
         new TotalOrderStateTransferFunctionalTest("dist-to-1pc-nbst-off-heap", CacheMode.DIST_SYNC, false, false, StorageType.OFF_HEAP),
         new TotalOrderStateTransferFunctionalTest("dist-to-2pc-nbst-off-heap", CacheMode.DIST_SYNC, false, true, StorageType.OFF_HEAP),
         new TotalOrderStateTransferFunctionalTest("repl-to-1pc-nbst", CacheMode.REPL_SYNC, true, true, StorageType.OBJECT),
         new TotalOrderStateTransferFunctionalTest("repl-to-2pc-nbst", CacheMode.REPL_SYNC, true, false, StorageType.OBJECT),
         new TotalOrderStateTransferFunctionalTest("repl-to-1pc-nbst-off-heap", CacheMode.REPL_SYNC, false, false, StorageType.OFF_HEAP),
         new TotalOrderStateTransferFunctionalTest("repl-to-1pc-nbst-off-heap", CacheMode.REPL_SYNC, false, true, StorageType.OFF_HEAP)
      };
   }

   public TotalOrderStateTransferFunctionalTest() {
      this(null, null, false, false, StorageType.OBJECT);
   }

   public TotalOrderStateTransferFunctionalTest(String cacheName, CacheMode mode, boolean writeSkew, boolean useSynchronization, StorageType storage) {
      super(cacheName);
      this.cacheMode = mode;
      this.writeSkew = writeSkew;
      this.useSynchronization = useSynchronization;
      this.storage = storage;
   }

   @Override
   protected String parameters() {
      return "[" + cacheName + "]";
   }

   protected void createCacheManagers() throws Throwable {
      configurationBuilder = getDefaultClusteredCacheConfig(cacheMode, true);
      configurationBuilder.transaction().transactionProtocol(TransactionProtocol.TOTAL_ORDER)
         .useSynchronization(useSynchronization)
         .recovery().disable();
      if (writeSkew) {
         configurationBuilder.locking().isolationLevel(IsolationLevel.REPEATABLE_READ);
      } else {
         configurationBuilder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      }
      configurationBuilder.clustering().stateTransfer().chunkSize(20);
      configurationBuilder.memory().storageType(storage);
   }

}
