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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.findInterceptor;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Test that commands are properly forwarded during/after state transfer.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "statetransfer.ReplCommandForwardingTest")
@CleanupAfterMethod
public class ReplCommandForwardingTest extends MultipleCacheManagersTest {

   @Override
   protected void createCacheManagers() {
      // do nothing, each test will create its own cache managers
   }

   private ConfigurationBuilder buildConfig(boolean transactional, boolean onePhase, Class<?> commandToBlock) {
      CacheMode mode = (transactional && !onePhase) ? CacheMode.REPL_SYNC : CacheMode.REPL_ASYNC;
      // The coordinator will always be the primary owner
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(mode, transactional);
      if (mode.isSynchronous()) configurationBuilder.clustering().sync().replTimeout(15000);
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      configurationBuilder.transaction().syncCommitPhase(false);
      // We must block after the commit was replicated, but before the entries are committed
      configurationBuilder.customInterceptors()
            .addInterceptor().after(EntryWrappingInterceptor.class).interceptor(new DelayInterceptor(commandToBlock));
      return configurationBuilder;
   }

   public void testForwardToJoinerNonTransactional() throws Exception {
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(false, false, PutKeyValueCommand.class));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);
      int initialTopologyId = extractComponent(c1, StateTransferManager.class).getCacheTopology().getTopologyId();

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(false, false, PutKeyValueCommand.class));
      Cache<Object, Object> c2 = cm2.getCache();
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

      // The put command is replicated to cache c2, and it blocks in the DelayInterceptor on both c1 and c2.
      di1.waitUntilBlocked(1);
      di2.waitUntilBlocked(1);

      // c3 joins, topology id changes
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(false, false, PutKeyValueCommand.class));
      Cache<Object, Object> c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 4, c1, c2, c3);

      // Unblock the replicated command on c2 and c1
      // Neither cache will forward the command to c3
      di2.unblock(1);
      di1.unblock(1);

      Thread.sleep(2000);
      assertEquals("The command shouldn't have been forwarded to " + c3, 0, di3.getCounter());

      log.tracef("Waiting for the put command to finish on %s", c1);
      Object retval = f.get(10, SECONDS);
      log.tracef("Put command finished on %s", c1);

      assertNull(retval);

      // 1 direct invocation
      assertEquals(1, di1.getCounter());
      // 1 from c1
      assertEquals(1, di2.getCounter());
      // 0 invocations
      assertEquals(0, di3.getCounter());
   }

   public void testForwardToJoinerAsyncPrepare() throws Exception {
      testForwardToJoinerAsyncTx(true);
   }

   public void testForwardToJoinerAsyncCommit() throws Exception {
      testForwardToJoinerAsyncTx(false);
   }

   protected void testForwardToJoinerAsyncTx(boolean onePhase) throws Exception {
      Class<?> commandToBlock = onePhase ? PrepareCommand.class : CommitCommand.class;
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(true, onePhase, commandToBlock));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);
      int initialTopologyId = extractComponent(c1, StateTransferManager.class).getCacheTopology().getTopologyId();

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(true, onePhase, commandToBlock));
      Cache c2 = cm2.getCache();
      DelayInterceptor di2 = findInterceptor(c2, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 2, c1, c2);

      Future<Object> f = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            log.tracef("Initiating a transaction on %s", c1);
            c1.put("k", "v");
            return null;
         }
      });

      // The prepare command is replicated to cache c2, and it blocks in the DelayInterceptor on c1 and c2
      di1.waitUntilBlocked(1);
      di2.waitUntilBlocked(1);

      // c3 joins, topology id changes
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(true, onePhase, commandToBlock));
      Cache c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 4, c1, c2, c3);

      // Unblock the replicated command on c2.
      // The StateTransferInterceptor on c2 will forward the command to c3.
      // The DelayInterceptor on c3 will then block, waiting for an unblock() call.
      log.tracef("Forwarding the prepare command from %s", c2);
      di2.unblock(1);
      di3.waitUntilBlocked(1);

      // c4 joins, topology id changes
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(buildConfig(true, onePhase, commandToBlock));
      Cache c4 = cm4.getCache();
      DelayInterceptor di4 = findInterceptor(c4, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 6, c1, c2, c3, c4);

      // Unblock the forwarded command on c3.
      // StateTransferInterceptor will then forward the command to c2 and c4.
      log.tracef("Forwarding the prepare command from %s", c3);
      di3.unblock(1);

      // Check that the c2 and c4 received the forwarded command.
      if (onePhase) {
         // Commit command would not execute a second time, because the remote tx was removed
         di2.unblock(2);
      }
      di4.unblock(1);

      // Allow the command to proceed on the originator (c1).
      // StateTransferInterceptor will forward the command to c2, c3, and c4.
      log.tracef("Forwarding the prepare command from %s", c1);
      di1.unblock(1);

      // Check that c2, c3, and c4 received the forwarded command.
      if (onePhase) {
         di2.unblock(3);
         di3.unblock(2);
         di4.unblock(2);
      }

      log.tracef("Waiting for the transaction to finish on %s", c1);
      f.get(10, SECONDS);
      log.tracef("Transaction finished on %s", c1);

      if (onePhase) {
         assertEquals(di1.getCounter(), 1);
         // 1 from replication + 1 re-forwarded by C + 1 forwarded by A
         assertEquals(di2.getCounter(), 3);
         // 1 forwarded by B + 1 forwarded by A
         assertEquals(di3.getCounter(), 2);
         // 1 re-1forwarded by C + 1 forwarded by A
         assertEquals(di4.getCounter(), 2);
      } else {
         // The commit is executed only once on all the nodes
         assertEquals(di1.getCounter(), 1);
         assertEquals(di2.getCounter(), 1);
         assertEquals(di3.getCounter(), 1);
         assertEquals(di4.getCounter(), 1);
      }
   }

   private void waitForStateTransfer(int expectedTopologyId, Cache... caches) {
      waitForRehashToComplete(caches);
      for (Cache c : caches) {
         CacheTopology cacheTopology = extractComponent(c, StateTransferManager.class).getCacheTopology();
         assertEquals(String.format("Wrong topology on cache %s, expected %d and got %s", c, expectedTopologyId,
               cacheTopology), cacheTopology.getTopologyId(), expectedTopologyId);
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
