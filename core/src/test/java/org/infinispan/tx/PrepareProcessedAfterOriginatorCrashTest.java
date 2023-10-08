package org.infinispan.tx;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.TestingUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.testng.annotations.Test;

/**
 * Tests the following scenario:
 * <pre>
 *  - tx1 originated on N1 writes a single key that maps to N2
 *  - tx1 prepares and before committing crashes
 *  - the prepare is blocked on N2 before the tx is created
 *  - TransactionTable.cleanupStaleTransactions kicks in on N2 but doesn't clean up the transaction
 *  as it hasn't been prepared yet
 *  - the prepare is now executed on N2
 *  - the test makes sure that the transaction doesn't acquire any locks and doesn't leak
 *  within N1
 *  </pre>
 *
 * @author Mircea Markus
 * @since 5.2
 */
@Test(groups = "functional", testName = "tx.PrepareProcessedAfterOriginatorCrashTest")
public class PrepareProcessedAfterOriginatorCrashTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder dcc = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      dcc.clustering().hash().numOwners(1);
      createClusteredCaches(2, TestDataSCI.INSTANCE, dcc);
   }

   public void testBelatedTransactionDoesntLeak() throws Throwable {
      CountDownLatch prepareReceived = new CountDownLatch(1);
      CountDownLatch prepareBlocked = new CountDownLatch(1);
      CountDownLatch prepareExecuted = new CountDownLatch(1);

      Cache receiver = cache(1);
      PerCacheInboundInvocationHandler originalInvocationHandler = TestingUtil.extractComponent(receiver, PerCacheInboundInvocationHandler.class);
      PerCacheInboundInvocationHandler blockingInvocationHandler = new AbstractDelegatingHandler(originalInvocationHandler) {
         @Override
         public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
            if (!(command instanceof PrepareCommand)) {
               delegate.handle(command, reply, order);
               return;
            }
            try {
               prepareReceived.countDown();
               prepareBlocked.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
               throw new IllegalLifecycleStateException(e);
            }
            log.trace("Processing belated prepare");
            delegate.handle(command, returnValue -> {
               prepareExecuted.countDown();
               reply.reply(returnValue);
            }, order);
         }
      };
      TestingUtil.replaceComponent(receiver, PerCacheInboundInvocationHandler.class, blockingInvocationHandler, true);
      TestingUtil.extractComponentRegistry(receiver).cacheComponents();

      final Object key = getKeyForCache(1);
      fork(() -> {
         try {
            cache(0).put(key, "v");
         } catch (Throwable e) {
            //possible as the node is being killed
         }
      });

      prepareReceived.await(10, TimeUnit.SECONDS);

      killMember(0);

      //give TransactionTable.cleanupStaleTransactions some time to run
      Thread.sleep(5000);

      prepareBlocked.countDown();

      prepareExecuted.await(10, TimeUnit.SECONDS);
      log.trace("Finished waiting for belated prepare to complete");

      final TransactionTable transactionTable = TestingUtil.getTransactionTable(receiver);
      assertEquals(0, transactionTable.getRemoteTxCount());
      assertEquals(0, transactionTable.getLocalTxCount());
      assertFalse(receiver.getAdvancedCache().getLockManager().isLocked(key));
   }
}
