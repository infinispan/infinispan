package org.infinispan.partitionhandling;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.extractLockManager;
import static org.infinispan.test.TestingUtil.getDiscardForCache;
import static org.infinispan.test.TestingUtil.getTransactionTable;
import static org.infinispan.test.TestingUtil.wrapGlobalComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.AdvancedCache;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commons.util.concurrent.CompletableFutures;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.conflict.MergePolicy;
import org.infinispan.distribution.DistributionInfo;
import org.infinispan.distribution.MagicKey;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManagerImpl;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestDataSCI;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.LockingMode;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.AbstractControlledLocalTopologyManager;
import org.infinispan.util.concurrent.locks.LockManager;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import jakarta.transaction.TransactionManager;

/**
 * Reproducer for ISPN-12757
 * <p>
 * It checks if the correct unlock method is invoked by the {@link PartitionHandlingManagerImpl} and no locks are
 * leaked.
 *
 * @author Pedro Ruivo
 * @since 12.1
 */
@Test(groups = "functional", testName = "partitionhandling.PessimisticTxPartitionHandlingReleaseLockTest")
public class PessimisticTxPartitionHandlingReleaseLockTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(PessimisticTxPartitionHandlingReleaseLockTest.class);

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction().lockingMode(LockingMode.PESSIMISTIC).useSynchronization(true);
      builder.clustering().partitionHandling().mergePolicy(MergePolicy.NONE).whenSplit(PartitionHandling.DENY_READ_WRITES);
      builder.clustering().remoteTimeout(4, TimeUnit.MINUTES); // we don't want timeouts
      createClusteredCaches(5, TestDataSCI.INSTANCE, builder, new TransportFlags().withFD(true).withMerge(true));
   }

   public void testLockReleased() throws Exception {
      final AdvancedCache<MagicKey, String> cache0 = this.<MagicKey, String>cache(0).getAdvancedCache();
      final TransactionManager tm = cache0.getTransactionManager();
      final ControlledLocalTopologyManager localTopologyManager0 = wrapGlobalComponent(cache0.getCacheManager(), LocalTopologyManager.class, ControlledLocalTopologyManager::new, true);
      final MagicKey key = new MagicKey(cache0);

      // Find the actual backup owner dynamically instead of hardcoding cache(1),
      // since the consistent hash may not place cache(1) as backup for any segment
      // where cache(0) is primary.
      DistributionInfo distributionInfo = cache0.getDistributionManager().getCacheTopology().getDistribution(key);
      int backupIndex = managerIndex(distributionInfo.writeBackups().iterator().next());
      final ControlledInboundHandler backupHandler = wrapInboundInvocationHandler(cache(backupIndex), ControlledInboundHandler::new);

      Future<GlobalTransaction> f = fork(() -> {
         tm.begin();
         cache0.lock(key);
         assertNull(cache0.get(key));
         GlobalTransaction gtx = getTransactionTable(cache0).getGlobalTransaction(tm.getTransaction());
         cache0.put(key, key.toString());
         tm.commit();
         return gtx;
      });

      //make sure the PrepareCommand was sent
      assertTrue(backupHandler.receivedLatch.await(30, TimeUnit.SECONDS));

      //block stable topology update on originator
      localTopologyManager0.blockStableTopologyUpdate();

      //isolate backup owner
      getDiscardForCache(manager(backupIndex)).discardAll(true);

      //wait for the transaction to finish
      //after the view change, the transaction is handled by the PartitionHandlingManager which finishes the commit
      final GlobalTransaction gtx = f.get();

      //check lock & pending transaction list
      Collection<GlobalTransaction> pendingTransactions = extractComponent(cache0, PartitionHandlingManager.class).getPartialTransactions();
      assertEquals(1, pendingTransactions.size());
      assertEquals(gtx, pendingTransactions.iterator().next());

      final LockManager lockManager0 = extractLockManager(cache0);
      assertTrue(lockManager0.isLocked(key));
      assertEquals(gtx, lockManager0.getOwner(key));

      //continue stable topology update
      localTopologyManager0.unblockStableTopologyUpdate();

      //eventually, the stable topology is installed which retries the prepare and releases the lock
      eventuallyEquals(0, lockManager0::getNumberOfLocksHeld);
   }

   private static class ControlledInboundHandler extends AbstractDelegatingHandler {

      private final CountDownLatch receivedLatch;

      private ControlledInboundHandler(PerCacheInboundInvocationHandler delegate) {
         super(delegate);
         receivedLatch = new CountDownLatch(1);
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof PrepareCommand) {
            log.debugf("Ignoring command %s", command);
            receivedLatch.countDown();
         } else {
            delegate.handle(command, reply, order);
         }
      }
   }

   public static class ControlledLocalTopologyManager extends AbstractControlledLocalTopologyManager {

      private volatile CompletableFuture<Void> block = CompletableFutures.completedNull();

      private ControlledLocalTopologyManager(LocalTopologyManager delegate) {
         super(delegate);
      }

      @Override
      protected CompletionStage<Void> beforeHandleTopologyUpdate(String cacheName, CacheTopology cacheTopology, int viewId) {
         return block;
      }

      private void blockStableTopologyUpdate() {
         block = new CompletableFuture<>();
      }

      private void unblockStableTopologyUpdate() {
         block.complete(null);
      }
   }
}
