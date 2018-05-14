package org.infinispan.statetransfer;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.infinispan.test.TestingUtil.findInterceptor;
import static org.infinispan.test.TestingUtil.waitForNoRebalance;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.control.LockControlCommand;
import org.infinispan.commands.tx.CommitCommand;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.FlagBitSets;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.interceptors.impl.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.util.ReplicatedControlledConsistentHashFactory;
import org.testng.annotations.Test;

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

   private ConfigurationBuilder buildConfig(Class<?> commandToBlock) {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig( CacheMode.REPL_ASYNC, false);
      configurationBuilder.clustering().remoteTimeout(15000);
      // The coordinator will always be the primary owner
      configurationBuilder.clustering().hash().numSegments(1).consistentHashFactory(new ReplicatedControlledConsistentHashFactory(0));
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      // We must block after the commit was replicated, but before the entries are committed
      configurationBuilder.customInterceptors()
            .addInterceptor().after(EntryWrappingInterceptor.class).interceptor(new DelayInterceptor(commandToBlock));
      return configurationBuilder;
   }

   public void testForwardToJoinerNonTransactional() throws Exception {
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(PutKeyValueCommand.class));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);
      int initialTopologyId = c1.getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(PutKeyValueCommand.class));
      Cache<Object, Object> c2 = cm2.getCache();
      DelayInterceptor di2 = findInterceptor(c2, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 4, c1, c2);

      // Start a 3rd node, but start a different cache there so that the topology stays the same.
      // Otherwise the put command blocked on node 1 could block the view message (as both are broadcast by node 0).
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(PutKeyValueCommand.class));
      cm3.defineConfiguration("differentCache", getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC).build());
      cm3.getCache("differentCache");

      Future<Object> f = fork(() -> {
         log.tracef("Initiating a put command on %s", c1);
         c1.put("k", "v");
         return null;
      });

      // The put command is replicated to cache c2, and it blocks in the DelayInterceptor on both c1 and c2.
      di1.waitUntilBlocked(1);
      di2.waitUntilBlocked(1);

      // c3 joins the cache, topology id changes
      Cache<Object, Object> c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(initialTopologyId + 8, c1, c2, c3);

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

   private void waitForStateTransfer(int expectedTopologyId, Cache... caches) {
      waitForNoRebalance(caches);
      for (Cache c : caches) {
         CacheTopology cacheTopology = c.getAdvancedCache().getDistributionManager().getCacheTopology();
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
         if (!ctx.isInTxScope() && !command.hasAnyFlag(FlagBitSets.PUT_FOR_STATE_TRANSFER)) {
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
