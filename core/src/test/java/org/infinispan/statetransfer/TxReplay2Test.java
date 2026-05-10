package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractInterceptorChain;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInterceptor;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.util.Arrays;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.remote.recovery.TxCompletionNotificationCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IsolationLevel;
import org.infinispan.configuration.internal.PrivateCacheConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.AsyncInterceptorChain;
import org.infinispan.interceptors.DDAsyncInterceptor;
import org.infinispan.interceptors.impl.CallInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.EmbeddedTransactionManagerLookup;
import org.infinispan.transaction.tm.EmbeddedTransaction;
import org.infinispan.transaction.tm.EmbeddedTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ByteString;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

import jakarta.transaction.Status;

/**
 * Tests that a transaction is replayed only once if the commit command is received twice.
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "functional", testName = "statetransfer.TxReplay2Test")
public class TxReplay2Test extends MultipleCacheManagersTest {
   private static final String VALUE = "value";

   ControlledConsistentHashFactory consistentHashFactory = new ControlledConsistentHashFactory.Default(0, 1, 2);

   public void testReplay() throws Exception {
      final StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:before_prepare_replay", "tx:resume_prepare_replay", "tx:mark_tx_completed");
      sequencer.logicalThread("sim", "sim:before_extra_commit", "sim:during_extra_commit", "sim:after_extra_commit");
      sequencer.order("tx:before_prepare_replay", "sim:before_extra_commit");
      sequencer.order("sim:during_extra_commit", "tx:resume_prepare_replay");
      sequencer.order("sim:after_extra_commit", "tx:mark_tx_completed");

      final Object key = "key";
      assertEquals(Arrays.asList(address(0), address(1), address(2)), cacheTopology(0).getDistribution(key).writeOwners());
      Cache<Object, Object> primaryOwnerCache = cache(0);
      final Cache<Object, Object> newBackupOwnerCache = cache(3);
      final CountingInterceptor newBackupCounter = CountingInterceptor.inject(newBackupOwnerCache);
      final CountingInterceptor primaryCounter = CountingInterceptor.inject(primaryOwnerCache);
      final CountingInterceptor oldBackup2Counter = CountingInterceptor.inject(cache(2));

      advanceOnInterceptor(sequencer, newBackupOwnerCache, CallInterceptor.class,
            matchCommand(PrepareCommand.class).matchCount(0).build())
            .before("tx:before_prepare_replay", "tx:resume_prepare_replay");
      advanceOnInterceptor(sequencer, newBackupOwnerCache, TransactionSynchronizerInterceptor.class,
            matchCommand(CommitCommand.class).matchCount(1).build())
            .before("sim:during_extra_commit");
      advanceOnInboundRpc(sequencer, newBackupOwnerCache, matchCommand(TxCompletionNotificationCommand.class).build())
            .before("tx:mark_tx_completed");

      final EmbeddedTransactionManager transactionManager = (EmbeddedTransactionManager) tm(0);
      transactionManager.begin();
      primaryOwnerCache.put(key, VALUE);

      final EmbeddedTransaction transaction = transactionManager.getTransaction();
      TransactionTable transactionTable0 = TestingUtil.getTransactionTable(primaryOwnerCache);
      final GlobalTransaction gtx = transactionTable0.getLocalTransaction(transaction).getGlobalTransaction();
      transaction.runPrepare();
      assertEquals(Status.STATUS_PREPARED, transaction.getStatus(), "Wrong transaction status before killing backup owner.");

      // Now, we kill cache(1). the transaction is prepared in cache(1) and it should be transferred to cache(2)
      killMember(1);

      int currentTopologyId = primaryOwnerCache.getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
      Future<Object> secondCommitFuture = fork(() -> {
         // Wait for the commit command to block replaying the prepare on the new backup
         sequencer.advance("sim:before_extra_commit");
         // And try to run another commit command
         CommitCommand command = new CommitCommand(ByteString.fromString(newBackupOwnerCache.getName()), gtx);
         command.setTopologyId(currentTopologyId);
         command.markTransactionAsRemote(true);
         ComponentRegistry componentRegistry = TestingUtil.extractComponentRegistry(newBackupOwnerCache);
         try {
            command.invokeAsync(componentRegistry);
         } catch (Throwable throwable) {
            throw new CacheException(throwable);
         }

         sequencer.advance("sim:after_extra_commit");
         return null;
      });

      checkIfTransactionExists(newBackupOwnerCache);
      assertEquals(Status.STATUS_PREPARED, transaction.getStatus(), "Wrong transaction status after killing backup owner.");
      transaction.runCommit(false);

      secondCommitFuture.get(10, SECONDS);

      assertNoTransactions();

      assertEquals(2, newBackupCounter.numberPrepares.get(), "Wrong number of prepares!");
      assertEquals(2, newBackupCounter.numberCommits.get(), "Wrong number of commits!");
      assertEquals(0, newBackupCounter.numberRollbacks.get(), "Wrong number of rollbacks!");

      assertEquals(2, oldBackup2Counter.numberPrepares.get(), "Wrong number of prepares!");
      assertEquals(1, oldBackup2Counter.numberCommits.get(), "Wrong number of commits!");
      assertEquals(0, oldBackup2Counter.numberRollbacks.get(), "Wrong number of rollbacks!");

      // We only count remote commands, and there shouldn't be any on the primary/originator
      assertEquals(0, primaryCounter.numberPrepares.get(), "Wrong number of prepares!");
      assertEquals(0, primaryCounter.numberCommits.get(), "Wrong number of commits!");
      assertEquals(0, primaryCounter.numberRollbacks.get(), "Wrong number of rollbacks!");

      checkKeyInDataContainer(key);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction()
            .useSynchronization(false)
            .transactionManagerLookup(new EmbeddedTransactionManagerLookup())
            .recovery().disable();
      builder.clustering()
            .hash().numOwners(3).numSegments(1)
            .stateTransfer().fetchInMemoryState(true);
      builder.addModule(PrivateCacheConfigurationBuilder.class).consistentHashFactory(consistentHashFactory);
      builder.locking().isolationLevel(IsolationLevel.READ_COMMITTED);
      createClusteredCaches(4, ControlledConsistentHashFactory.SCI.INSTANCE, builder);
   }

   private void checkKeyInDataContainer(Object key) {
      for (Cache<Object, Object> cache : caches()) {
         DataContainer container = cache.getAdvancedCache().getDataContainer();
         InternalCacheEntry entry = container.peek(key);
         assertNotNull(entry, "Cache '" + address(cache) + "' does not contain key!");
         assertEquals(VALUE, entry.getValue(), "Cache '" + address(cache) + "' has wrong value!");
      }
   }

   private void checkIfTransactionExists(Cache<Object, Object> cache) {
      TransactionTable table = TestingUtil.extractComponent(cache, TransactionTable.class);
      assertFalse(table.getRemoteTransactions().isEmpty(), "Expected a remote transaction.");
   }

   static class CountingInterceptor extends DDAsyncInterceptor {
      private static final Log log = LogFactory.getLog(CountingInterceptor.class);

      //counters
      private final AtomicInteger numberPrepares = new AtomicInteger(0);
      private final AtomicInteger numberCommits = new AtomicInteger(0);
      private final AtomicInteger numberRollbacks = new AtomicInteger(0);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            log.debugf("Received remote prepare for transaction %s", command.getGlobalTransaction());
            numberPrepares.incrementAndGet();
         }
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            log.debugf("Received remote commit for transaction %s", command.getGlobalTransaction());
            numberCommits.incrementAndGet();
         }
         return invokeNext(ctx, command);
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            log.debugf("Received remote rollback for transaction %s", command.getGlobalTransaction());
            numberRollbacks.incrementAndGet();
         }
         return invokeNext(ctx, command);
      }

      public static CountingInterceptor inject(Cache cache) {
         AsyncInterceptorChain chain = extractInterceptorChain(cache);
         if (chain.containsInterceptorType(CountingInterceptor.class)) {
            return chain.findInterceptorWithClass(CountingInterceptor.class);
         }
         CountingInterceptor interceptor = new CountingInterceptor();
         chain.addInterceptorBefore(interceptor, CallInterceptor.class);
         return interceptor;
      }
   }
}
