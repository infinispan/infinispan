package org.infinispan.tx.totalorder.statetransfer;

import java.lang.reflect.Method;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.statetransfer.StateTransferFunctionalTest;
import org.infinispan.transaction.TransactionProtocol;
import org.infinispan.util.concurrent.IsolationLevel;
import org.testng.annotations.Test;

/**
 * @author Mircea Markus <mircea.markus@jboss.com> (C) 2011 Red Hat Inc.
 * @since 5.3
 */
@Test(groups = "unstable", testName = "tx.totalorder.statetransfer.TotalOrderStateTransferFunctionalTest", description = "JGRP-2233")
public class TotalOrderStateTransferFunctionalTest extends StateTransferFunctionalTest {

   protected final boolean writeSkew;
   protected final boolean useSynchronization;

   @Override
   public Object[] factory() {
      return new Object[] {
         new TotalOrderStateTransferFunctionalTest("dist-to-1pc-nbst", CacheMode.DIST_SYNC, false, false),
         new TotalOrderStateTransferFunctionalTest("dist-to-2pc-nbst", CacheMode.DIST_SYNC, true, false),
         new TotalOrderStateTransferFunctionalTest("repl-to-1pc-nbst", CacheMode.REPL_SYNC, true, true),
         new TotalOrderStateTransferFunctionalTest("repl-to-2pc-nbst", CacheMode.REPL_SYNC, true, false),
      };
   }

   public TotalOrderStateTransferFunctionalTest() {
      this(null, null, false, false);
   }

   public TotalOrderStateTransferFunctionalTest(String cacheName, CacheMode mode, boolean writeSkew, boolean useSynchronization) {
      super(cacheName);
      this.cacheMode = mode;
      this.writeSkew = writeSkew;
      this.useSynchronization = useSynchronization;
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
   }

   @Override
   public void testInitialStateTransfer(Method m) throws Exception {
      super.testInitialStateTransfer(m);    // TODO: Customise this generated block
   }

   @Override
   public void testInitialStateTransferCacheNotPresent(Method m) throws Exception {
      super.testInitialStateTransferCacheNotPresent(m);    // TODO: Customise this generated block
   }

   @Override
   public void testConcurrentStateTransfer(Method m) throws Exception {
      super.testConcurrentStateTransfer(m);    // TODO: Customise this generated block
   }

   @Override
   public void testSTWithThirdWritingNonTxCache(Method m) throws Exception {
      super.testSTWithThirdWritingNonTxCache(m);    // TODO: Customise this generated block
   }

   @Override
   public void testSTWithThirdWritingTxCache(Method m) throws Exception {
      super.testSTWithThirdWritingTxCache(m);    // TODO: Customise this generated block
   }

   @Override
   public void testSTWithWritingNonTxThread(Method m) throws Exception {
      super.testSTWithWritingNonTxThread(m);    // TODO: Customise this generated block
   }

   @Override
   public void testSTWithWritingTxThread(Method m) throws Exception {
      super.testSTWithWritingTxThread(m);    // TODO: Customise this generated block
   }

   @Override
   public void testInitialStateTransferAfterRestart(Method m) throws Exception {
      super.testInitialStateTransferAfterRestart(m);    // TODO: Customise this generated block
   }
}
