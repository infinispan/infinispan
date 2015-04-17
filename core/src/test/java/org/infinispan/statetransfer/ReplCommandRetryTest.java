package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.findInterceptor;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Test that commands are properly retried during/after state transfer.
 *
 * @author Dan Berindei
 * @since 7.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplCommandRetryTest")
@CleanupAfterMethod
public class ReplCommandRetryTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      // do nothing, each test will create its own cache managers
   }

   private ConfigurationBuilder buildConfig(LockingMode lockingMode, Class<?> commandToBlock, boolean isOriginator) {
      // The coordinator will always be the primary owner
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, lockingMode != null);
      configurationBuilder.transaction().lockingMode(lockingMode);
      configurationBuilder.clustering().sync().replTimeout(15000);
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      if (commandToBlock == LockControlCommand.class && !isOriginator) {
         configurationBuilder.customInterceptors().addInterceptor()
               .before(PessimisticLockingInterceptor.class).interceptor(new DelayInterceptor(commandToBlock));
      } else {
         configurationBuilder.customInterceptors().addInterceptor()
               .after(EntryWrappingInterceptor.class).interceptor(new DelayInterceptor(commandToBlock));
      }
      return configurationBuilder;
   }

   public void testRetryAfterJoinNonTransactional() throws Exception {
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(null, PutKeyValueCommand.class, true));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);
      int initialTopologyId = extractComponent(c1, StateTransferManager.class).getCacheTopology().getTopologyId();

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(null, PutKeyValueCommand.class, false));
      final Cache<Object, Object> c2 = cm2.getCache();
      DelayInterceptor di2 = findInterceptor(c2, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 2, c1, c2);

      Future<Object> f = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            log.tracef("Initiating a put command on %s", c1);
            c1.put("k", "v");
            return null;
         }
      });

      // The command is replicated to c2, and blocks in the DelayInterceptor on c2
      di2.waitUntilBlocked(1);

      // c3 joins, topology id changes
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(null, PutKeyValueCommand.class, false));
      Cache<Object, Object> c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 4, c1, c2, c3);

      // Unblock the replicated command on c2.
      log.tracef("Triggering retry 1");
      di2.unblock(1);

      // c2 will return UnsureResponse, and c1 will retry the command.
      // c1 will send the command to c2 and c3, blocking on both in the DelayInterceptor
      di2.waitUntilBlocked(2);
      di3.waitUntilBlocked(1);

      // Unblock the command with the new topology id on c2
      di2.unblock(2);

      // c4 joins, topology id changes
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(buildConfig(null, PutKeyValueCommand.class, false));
      Cache<Object, Object> c4 = cm4.getCache();
      DelayInterceptor di4 = findInterceptor(c4, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 6, c1, c2, c3, c4);

      // Unblock the command with the new topology id on c3.
      log.tracef("Triggering retry 2");
      di3.unblock(1);

      // c3 will send an UnsureResponse, and c1 will retry the command.
      // c1 will send the command to c2, c3, and c4, blocking everywhere in the DelayInterceptor
      // Unblock every node except c1
      di2.unblock(3);
      di3.unblock(2);
      di4.unblock(1);

      // Now c1 blocks
      di1.unblock(1);

      log.tracef("Waiting for the put command to finish on %s", c1);
      Object retval = f.get(10, TimeUnit.SECONDS);
      log.tracef("Put command finished on %s", c1);

      assertNull(retval);

      // 1 for the last retry
      assertEquals(1, di1.getCounter());
      // 1 for the initial invocation + 1 for each retry
      assertEquals(3, di2.getCounter());
      // 1 for each retry
      assertEquals(2, di3.getCounter());
      // just the last retry
      assertEquals(1, di4.getCounter());
   }

   public void testRetryAfterJoinLockControlCommand() throws Exception {
      testRetryAfterJoinTransactional(LockingMode.PESSIMISTIC, LockControlCommand.class);
   }

   public void testRetryAfterJoinOnePhasePrepareCommand() throws Exception {
      testRetryAfterJoinTransactional(LockingMode.PESSIMISTIC, PrepareCommand.class);
   }

   public void testRetryAfterJoinTwoPhasePrepareCommand() throws Exception {
      testRetryAfterJoinTransactional(LockingMode.OPTIMISTIC, PrepareCommand.class);
   }

   public void testRetryAfterJoinCommitCommand() throws Exception {
      testRetryAfterJoinTransactional(LockingMode.OPTIMISTIC, CommitCommand.class);
   }

   private void testRetryAfterJoinTransactional(LockingMode lockingMode, Class<?> commandClass) throws Exception {
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(lockingMode, commandClass, false));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);
      int initialTopologyId = extractComponent(c1, StateTransferManager.class).getCacheTopology().getTopologyId();

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(lockingMode, commandClass, true));
      final Cache c2 = cm2.getCache();
      DelayInterceptor di2 = findInterceptor(c2, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 2, c1, c2);

      Future<Object> f = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            // The LockControlCommand wouldn't be replicated if we initiated the transaction on the primary owner (c1)
            log.tracef("Initiating a transaction on backup owner %s", c2);
            c2.put("k", "v");
            return null;
         }
      });

      // The prepare command is replicated to cache c1, and it blocks in the DelayInterceptor
      di1.waitUntilBlocked(1);

      // c3 joins, topology id changes
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(lockingMode, commandClass, false));
      Cache c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 4, c1, c2, c3);

      // Unblock the replicated command on c1.
      // c1 will return an UnsureResponse, and c2 will retry (1)
      log.tracef("Triggering retry 1 from node %s", c1);
      di1.unblock(1);

      // The prepare command will again block on c1 and c3
      di1.waitUntilBlocked(2);
      di3.waitUntilBlocked(1);

      // c4 joins, topology id changes
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(buildConfig(lockingMode, commandClass, false));
      Cache c4 = cm4.getCache();
      DelayInterceptor di4 = findInterceptor(c4, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 6, c1, c2, c3, c4);

      // Unblock the replicated command on c1
      di1.unblock(2);

      // Unblock the replicated command on c3, c2 will retry (2)
      log.tracef("Triggering retry 2 from %s", c3);
      di3.unblock(1);

      // Check that the c1, c3, and c4 all received the retried command
      di1.unblock(3);
      di3.unblock(2);
      di4.unblock(1);

      // Allow the command to finish on the originator (c2).
      log.tracef("Finishing tx on %s", c2);
      di2.unblock(1);

      log.tracef("Waiting for the transaction to finish on %s", c2);
      f.get(10, TimeUnit.SECONDS);
      log.tracef("Transaction finished on %s", c2);

      // 1 for the initial call + 1 for each retry (2)
      assertEquals(di1.getCounter(), 3);
      // 1 for the last retry
      assertEquals(di2.getCounter(), 1);
      // 1 for each retry
      assertEquals(di3.getCounter(), 2);
      // 1 for the last retry
      assertEquals(di4.getCounter(), 1);
   }

   private void waitForStateTransfer(int expectedTopologyId, Cache... caches) {
      waitForRehashToComplete(caches);
      for (Cache c : caches) {
         CacheTopology cacheTopology = extractComponent(c, StateTransferManager.class).getCacheTopology();
         assertEquals(String.format("Wrong topology on cache %s, expected %d and got %s", c, expectedTopologyId, cacheTopology),
               expectedTopologyId, cacheTopology.getTopologyId());
      }
   }

   private class DelayInterceptor extends BaseCustomInterceptor {
      private final AtomicInteger counter = new AtomicInteger(0);
      private final CheckPoint checkPoint = new CheckPoint();
      private final Class<?> commandToBlock;

      public DelayInterceptor(Class<?> commandToBlock) {
         this.commandToBlock = commandToBlock;
      }

      public int getCounter() {
         return counter.get();
      }

      public void waitUntilBlocked(int count) throws TimeoutException, InterruptedException {
         String event = checkPoint.peek(5, SECONDS, "blocked_" + count + "_on_" + cache);
         assertEquals("blocked_" + count + "_on_" + cache, event);
      }

      public void unblock(int count) throws InterruptedException, TimeoutException, BrokenBarrierException {
         log.tracef("Unblocking command on cache %s", cache);
         checkPoint.awaitStrict("blocked_" + count + "_on_" + cache, 5, SECONDS);
         checkPoint.trigger("resume_" + count + "_on_" + cache);
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         Object result = super.visitPutKeyValueCommand(ctx, command);
         if (!ctx.isInTxScope() && !command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
            doBlock(ctx, command);
         }
         return result;
      }

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command) throws Throwable {
         Object result = super.visitLockControlCommand(ctx, command);
         if (!ctx.getCacheTransaction().isFromStateTransfer()) {
            doBlock(ctx, command);
         }
         return result;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         Object result = super.visitPrepareCommand(ctx, command);
         if (!ctx.getCacheTransaction().isFromStateTransfer()) {
            doBlock(ctx, command);
         }
         return result;
      }

      @Override
      public Object visitCommitCommand(TxInvocationContext ctx, CommitCommand command) throws Throwable {
         Object result = super.visitCommitCommand(ctx, command);
         if (!ctx.getCacheTransaction().isFromStateTransfer()) {
            doBlock(ctx, command);
         }
         return result;
      }

      private void doBlock(InvocationContext ctx, ReplicableCommand command) throws InterruptedException,
            TimeoutException {
         if (commandToBlock != command.getClass())
            return;

         log.tracef("Delaying command %s originating from %s", command, ctx.getOrigin());
         Integer myCount = counter.incrementAndGet();
         checkPoint.trigger("blocked_" + myCount + "_on_" + cache);
         checkPoint.awaitStrict("resume_" + myCount + "_on_" + cache, 15, SECONDS);
         log.tracef("Command unblocked: %s", command);
      }

      @Override
      public String toString() {
         return "DelayInterceptor{counter=" + counter + "}";
      }
   }
}
