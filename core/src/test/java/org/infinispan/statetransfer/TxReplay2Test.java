package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.CommandsFactory;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.tx.RollbackCommand;
import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.interceptors.CallInterceptor;
import org.infinispan.interceptors.InterceptorChain;
import org.infinispan.interceptors.base.CommandInterceptor;
import org.infinispan.remoting.InboundInvocationHandler;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.concurrent.StateSequencerUtil;
import org.infinispan.transaction.impl.TransactionTable;
import org.infinispan.transaction.lookup.DummyTransactionManagerLookup;
import org.infinispan.transaction.tm.DummyTransaction;
import org.infinispan.transaction.tm.DummyTransactionManager;
import org.infinispan.transaction.xa.GlobalTransaction;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.jgroups.Message;
import org.jgroups.blocks.Response;
import org.testng.annotations.Test;

import javax.transaction.Status;
import java.util.Arrays;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInterceptor;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Tests that a transaction is replayed only once if the commit command is received twice.
 *
 * @author Dan Berindei
 * @since 7.0
 */
@Test(groups = "functional", testName = "statetransfer.TxReplay2Test")
public class TxReplay2Test extends MultipleCacheManagersTest {
   private static final String VALUE = "value";

   ControlledConsistentHashFactory consistentHashFactory = new ControlledConsistentHashFactory(0, 1, 2);

   public void testReplay() throws Exception {
      final StateSequencer sequencer = new StateSequencer();
      sequencer.logicalThread("tx", "tx:before_prepare_replay", "tx:resume_prepare_replay");
      sequencer.logicalThread("sim", "sim:before_extra_commit", "sim:during_extra_commit");
      sequencer.order("tx:before_prepare_replay", "sim:before_extra_commit", "sim:during_extra_commit", "tx:resume_prepare_replay");

      final Object key = "key";
      assertEquals(Arrays.asList(address(0), address(1), address(2)), advancedCache(0).getDistributionManager().locate(key));
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

      final DummyTransactionManager transactionManager = (DummyTransactionManager) tm(0);
      transactionManager.begin();
      primaryOwnerCache.put(key, VALUE);

      final DummyTransaction transaction = transactionManager.getTransaction();
      TransactionTable transactionTable0 = TestingUtil.getTransactionTable(primaryOwnerCache);
      final GlobalTransaction gtx = transactionTable0.getLocalTransaction(transaction).getGlobalTransaction();
      transaction.runPrepare();
      assertEquals("Wrong transaction status before killing backup owner.",
            Status.STATUS_PREPARED, transaction.getStatus());

      // Now, we kill cache(1). the transaction is prepared in cache(1) and it should be transferred to cache(2)
      killMember(1);

      final int currentTopologyId = TestingUtil.extractComponentRegistry(primaryOwnerCache).getStateTransferManager().getCacheTopology().getTopologyId();
      Future<Object> secondCommitFuture = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            // Wait for the commit command to block replaying the prepare on the new backup
            sequencer.advance("sim:before_extra_commit");
            // And try to run another commit command
            CommitCommand command = new CommitCommand(newBackupOwnerCache.getName(), gtx);
            command.setTopologyId(currentTopologyId);
            CommandsFactory cf = TestingUtil.extractCommandsFactory(newBackupOwnerCache);
            cf.initializeReplicableCommand(command, true);
            try {
               command.perform(null);
            } catch (Throwable throwable) {
               throw new CacheException(throwable);
            }

            return null;
         }
      });

      checkIfTransactionExists(newBackupOwnerCache);
      assertEquals("Wrong transaction status after killing backup owner.",
            Status.STATUS_PREPARED, transaction.getStatus());
      transaction.runCommitTx();

      secondCommitFuture.get(10, SECONDS);

      assertNoTransactions();

      assertEquals("Wrong number of prepares!", 1, newBackupCounter.numberPrepares.get());
      assertEquals("Wrong number of commits!", 1, newBackupCounter.numberCommits.get());
      assertEquals("Wrong number of rollbacks!", 0, newBackupCounter.numberRollbacks.get());

      assertEquals("Wrong number of prepares!", 2, oldBackup2Counter.numberPrepares.get());
      assertEquals("Wrong number of commits!", 1, oldBackup2Counter.numberCommits.get());
      assertEquals("Wrong number of rollbacks!", 0, oldBackup2Counter.numberRollbacks.get());

      // We only count remote commands, and there shouldn't be any on the primary/originator
      assertEquals("Wrong number of prepares!", 0, primaryCounter.numberPrepares.get());
      assertEquals("Wrong number of commits!", 0, primaryCounter.numberCommits.get());
      assertEquals("Wrong number of rollbacks!", 0, primaryCounter.numberRollbacks.get());

      checkKeyInDataContainer(key);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, true);
      builder.transaction()
            .useSynchronization(false)
            .transactionManagerLookup(new DummyTransactionManagerLookup())
            .recovery().disable();
      builder.clustering()
            .hash().numOwners(3).numSegments(1).consistentHashFactory(consistentHashFactory)
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(4, builder);
   }

   private void checkKeyInDataContainer(Object key) {
      for (Cache<Object, Object> cache : caches()) {
         DataContainer container = cache.getAdvancedCache().getDataContainer();
         InternalCacheEntry entry = container.get(key);
         assertNotNull("Cache '" + address(cache) + "' does not contain key!", entry);
         assertEquals("Cache '" + address(cache) + "' has wrong value!", VALUE, entry.getValue());
      }
   }

   private void checkIfTransactionExists(Cache<Object, Object> cache) {
      TransactionTable table = TestingUtil.extractComponent(cache, TransactionTable.class);
      assertFalse("Expected a remote transaction.", table.getRemoteTransactions().isEmpty());
   }

   private static class CountingInterceptor extends CommandInterceptor {
      //counters
      private final AtomicInteger numberPrepares = new AtomicInteger(0);
      private final AtomicInteger numberCommits = new AtomicInteger(0);
      private final AtomicInteger numberRollbacks = new AtomicInteger(0);

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            numberPrepares.incrementAndGet();
         }
         return invokeNextInterceptor(ctx, command);
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            numberCommits.incrementAndGet();
         }
         return invokeNextInterceptor(ctx, command);
      }

      @Override
      public Object visitRollbackCommand(TxInvocationContext ctx, RollbackCommand command) throws Throwable {
         if (!ctx.isOriginLocal()) {
            numberRollbacks.incrementAndGet();
         }
         return invokeNextInterceptor(ctx, command);
      }

      public static CountingInterceptor inject(Cache cache) {
         InterceptorChain chain = TestingUtil.extractComponent(cache, InterceptorChain.class);
         if (chain.containsInterceptorType(CountingInterceptor.class)) {
            return (CountingInterceptor) chain.getInterceptorsWithClass(CountingInterceptor.class).get(0);
         }
         CountingInterceptor interceptor = new CountingInterceptor();
         chain.addInterceptorBefore(interceptor, CallInterceptor.class);
         return interceptor;
      }
   }
}
