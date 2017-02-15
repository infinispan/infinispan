package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.waitForStableTopology;
import static org.testng.AssertJUnit.assertEquals;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.TransactionManager;

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
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.locking.PessimisticLockingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.LockingMode;
import org.testng.annotations.Test;

/**
 * Test that commands are properly retried during/after state transfer.
 *
 * @author Dan Berindei
 * @since 8.2
 */
@Test(groups = "functional", testName = "statetransfer.GetWithForceWriteLockRetryTest")
@CleanupAfterMethod
public class GetWithForceWriteLockRetryTest extends MultipleCacheManagersTest {

   public static final int CLUSTER_SIZE = 3;

   @Override
   protected void createCacheManagers() {
      for (int i = 0; i < CLUSTER_SIZE; i++) {
         addClusterEnabledCacheManager(buildConfig());
      }
      waitForClusterToForm();
   }

   private ConfigurationBuilder buildConfig() {
      // The coordinator will always be the primary owner
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      configurationBuilder.clustering().hash().numSegments(60);
      configurationBuilder.transaction().lockingMode(LockingMode.PESSIMISTIC);
      return configurationBuilder;
   }

   public void testRetryAfterLeave() throws Exception {
      EmbeddedCacheManager cm1 = manager(0);
      Cache<Object, Object> c1 = cm1.getCache();

      EmbeddedCacheManager cm2 = manager(1);
      Cache c2 = cm2.getCache();

      EmbeddedCacheManager cm3 = manager(2);
      Cache c3 = cm3.getCache();
      DelayInterceptor di3 = new DelayInterceptor(LockControlCommand.class);
      c3.getAdvancedCache().addInterceptorBefore(di3, PessimisticLockingInterceptor.class);

      Object key = new MagicKey(c3);
      TransactionManager tm1 = tm(c1);
      Future<Object> f = fork(() -> {
         log.tracef("Initiating a transaction on backup owner %s", c2);
         tm1.begin();
         try {
            c1.getAdvancedCache().withFlags(Flag.FORCE_WRITE_LOCK).get(key);
         } finally {
            // Even if the remote lock failed, this will remove the transaction
            tm1.commit();
         }
         return null;
      });

      // The prepare command is replicated to cache c1, and it blocks in the DelayInterceptor
      di3.waitUntilBlocked(1);

      // Kill c3
      killMember(2);
      waitForStableTopology(c1, c2);

      // Check that the lock succeeded
      f.get(10, SECONDS);

      // Unblock the remote command on c3 - shouldn't make any difference
      di3.unblock(1);
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
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command)
            throws Throwable {
         Object result = super.visitPutKeyValueCommand(ctx, command);
         if (!ctx.isInTxScope() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
            doBlock(ctx, command);
         }
         return result;
      }

      @Override
      public Object visitLockControlCommand(TxInvocationContext ctx, LockControlCommand command)
            throws Throwable {
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

      private void doBlock(InvocationContext ctx, ReplicableCommand command)
            throws InterruptedException, TimeoutException {
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
