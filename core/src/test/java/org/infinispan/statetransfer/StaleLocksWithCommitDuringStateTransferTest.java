package org.infinispan.statetransfer;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.transaction.impl.LocalTransaction;
import org.infinispan.transaction.impl.TransactionCoordinator;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

@Test(testName = "statetransfer.StaleLocksWithCommitDuringStateTransferTest", groups = "functional")
@CleanupAfterMethod
public class StaleLocksWithCommitDuringStateTransferTest extends MultipleCacheManagersTest {

   Cache<MagicKey, String> c1, c2;

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cb = new ConfigurationBuilder();
      cb.clustering().cacheMode(CacheMode.DIST_SYNC)
            .sync().replTimeout(5000)
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL).cacheStopTimeout(100);
      EmbeddedCacheManager cm1 = TestCacheManagerFactory.createClusteredCacheManager(cb);
      EmbeddedCacheManager cm2 = TestCacheManagerFactory.createClusteredCacheManager(cb);
      registerCacheManager(cm1, cm2);
      c1 = cm1.getCache();
      c2 = cm2.getCache();
      waitForClusterToForm();
   }

   public void testRollbackLocalFailure() throws Exception {
      doStateTransferInProgressTest(false, true);
   }

   public void testCommitLocalFailure() throws Exception {
      doStateTransferInProgressTest(true, true);
   }

   public void testRollbackRemoteFailure() throws Exception {
      doStateTransferInProgressTest(false, false);
   }

   public void testCommitRemoteFailure() throws Exception {
      doStateTransferInProgressTest(true, false);
   }

   /**
    * Check that the transaction commit/rollback recovers if we receive a StateTransferInProgressException from the remote node
    */
   private void doStateTransferInProgressTest(boolean commit, final boolean failOnOriginator) throws Exception {
      MagicKey k1 = new MagicKey("k1", c1);
      MagicKey k2 = new MagicKey("k2", c2);

      tm(c1).begin();
      c1.put(k1, "v1");
      c1.put(k2, "v2");

      // We split the transaction commit in two phases by calling the TransactionCoordinator methods directly
      TransactionTable txTable = TestingUtil.extractComponent(c1, TransactionTable.class);
      TransactionCoordinator txCoordinator = TestingUtil.extractComponent(c1, TransactionCoordinator.class);

      // Execute the prepare on both nodes
      LocalTransaction localTx = txTable.getLocalTransaction(tm(c1).getTransaction());
      txCoordinator.prepare(localTx);

      final CountDownLatch commitLatch = new CountDownLatch(1);
      Thread worker = new Thread("RehasherSim,StaleLocksWithCommitDuringStateTransferTest") {
         @Override
         public void run() {
            try {
               // Before calling commit we block transactions on one of the nodes to simulate a state transfer
               final StateTransferLock blockFirst = TestingUtil.extractComponent(failOnOriginator ? c1 : c2, StateTransferLock.class);
               final StateTransferLock blockSecond = TestingUtil.extractComponent(failOnOriginator ? c2 : c1, StateTransferLock.class);

               try {
                  blockFirst.acquireExclusiveTopologyLock();
                  blockSecond.acquireExclusiveTopologyLock();

                  commitLatch.countDown();

                  // should be much larger than the lock acquisition timeout
                  Thread.sleep(1000);
               } finally {
                  blockSecond.releaseExclusiveTopologyLock();
                  blockFirst.releaseExclusiveTopologyLock();
               }
            } catch (Throwable t) {
               log.errorf(t, "Error blocking/unblocking transactions");
            }
         }
      };
      worker.start();

      commitLatch.await(10, TimeUnit.SECONDS);

      try {
         // finally commit or rollback the transaction
         if (commit) {
            tm(c1).commit();
         } else {
            tm(c1).rollback();
         }

         // make the transaction manager forget about our tx so that we don't get rollback exceptions in the log
         tm(c1).suspend();
      } finally {
         // don't leak threads
         worker.join();
      }

      // test that we don't leak locks
      assertEventuallyNotLocked(c1, k1);
      assertEventuallyNotLocked(c2, k1);
      assertEventuallyNotLocked(c1, k2);
      assertEventuallyNotLocked(c2, k2);
   }

   public void testRollbackSuspectFailure() throws Exception {
      doTestSuspect(false);
   }

   public void testCommitSuspectFailure() throws Exception {
      doTestSuspect(true);
   }

   /**
    * Check that the transaction commit/rollback recovers if the remote node dies during the RPC
    */
   private void doTestSuspect(boolean commit) throws Exception {
      MagicKey k1 = new MagicKey("k1", c1);
      MagicKey k2 = new MagicKey("k2", c2);

      tm(c1).begin();
      c1.put(k1, "v1");
      c1.put(k2, "v2");

      // We split the transaction commit in two phases by calling the TransactionCoordinator methods directly
      TransactionTable txTable = TestingUtil.extractComponent(c1, TransactionTable.class);
      TransactionCoordinator txCoordinator = TestingUtil.extractComponent(c1, TransactionCoordinator.class);

      // Execute the prepare on both nodes
      LocalTransaction localTx = txTable.getLocalTransaction(tm(c1).getTransaction());
      txCoordinator.prepare(localTx);

      // Delay the commit on the remote node. Can't used blockNewTransactions because we don't want a StateTransferInProgressException
      InterceptorChain c2ic = TestingUtil.extractComponent(c2, InterceptorChain.class);
      c2ic.addInterceptorBefore(new CommandInterceptor() {
         @Override
         protected Object handleDefault(InvocationContext ctx, VisitableCommand command) throws Throwable {
            if (command instanceof CommitCommand) {
               Thread.sleep(3000);
            }
            return super.handleDefault(ctx, command);
         }
      }, StateTransferInterceptor.class);

      // Schedule the remote node to stop on another thread since the main thread will be busy with the commit call
      Thread worker = new Thread("RehasherSim,StaleLocksWithCommitDuringStateTransferTest") {
         @Override
         public void run() {
            try {
               // should be much larger than the lock acquisition timeout
               Thread.sleep(1000);
               manager(c2).stop();
               // stLock.unblockNewTransactions(1000);
            } catch (InterruptedException e) {
               log.errorf(e, "Error stopping cache");
            }
         }
      };
      worker.start();

      try {
         // finally commit or rollback the transaction
         if (commit) {
            txCoordinator.commit(localTx, false);
         } else {
            txCoordinator.rollback(localTx);
         }

         // make the transaction manager forget about our tx so that we don't get rollback exceptions in the log
         tm(c1).suspend();
      } finally {
         // don't leak threads
         worker.join();
      }

      // test that we don't leak locks
      assertEventuallyNotLocked(c1, k1);
      assertEventuallyNotLocked(c1, k2);
   }
}
