package org.infinispan.statetransfer;

import org.infinispan.Cache;
import org.infinispan.commands.tx.PrepareCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.context.InvocationContext;
import org.infinispan.context.impl.TxInvocationContext;
import org.infinispan.interceptors.base.BaseCustomInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.transaction.impl.LocalTransaction;
import org.testng.annotations.Test;

import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.infinispan.test.TestingUtil.extractComponent;
import static org.infinispan.test.TestingUtil.findInterceptor;
import static org.infinispan.test.TestingUtil.waitForRehashToComplete;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertNull;

/**
 * Test that command forwarding works during/after state transfer.
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

   private ConfigurationBuilder buildConfig(boolean transactional) {
      ConfigurationBuilder configurationBuilder = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, transactional);
      configurationBuilder.clustering().sync().replTimeout(15000);
      configurationBuilder.clustering().stateTransfer().fetchInMemoryState(true);
      configurationBuilder.customInterceptors().addInterceptor().after(StateTransferInterceptor.class).interceptor(new DelayInterceptor());
      return configurationBuilder;
   }

   public void testForwardToJoinerNonTransactional() throws Exception {
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(false));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(false));
      Cache<Object, Object> c2 = cm2.getCache();
      DelayInterceptor di2 = findInterceptor(c2, DelayInterceptor.class);
      waitForStateTransfer(2, c1, c2);

      Future<Object> f = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            log.tracef("Initiating a put command on %s", c1);
            // The put command is replicated to cache c2, and it blocks in the DelayInterceptor.
            c1.put("k", "v");
            return null;
         }
      });

      // Unblock the command on c1, so that it is replicated to c2
      di1.unblock(1);

      // c3 joins, topology id changes
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(false));
      Cache<Object, Object> c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(4, c1, c2, c3);

      // Unblock the replicated command on c2.
      // NonTxConcurrentDistributionInterceptor on c3 will throw an OutdatedTopologyException,
      // and StateTransferInterceptor on c1 will retry the command.
      // The DelayInterceptor on c1 will then block, waiting for an unblock() call.
      di2.unblock(1);

      // Unblock the command with the new topology id on c1
      log.tracef("Retrying the command on %s", c1);
      di1.unblock(2);

      // Unblock the command with the new topology id from c1 on c2 (c2 won't throw an exception again).
      di2.unblock(2);

      // c4 joins, topology id changes
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(buildConfig(false));
      Cache<Object, Object> c4 = cm4.getCache();
      DelayInterceptor di4 = findInterceptor(c4, DelayInterceptor.class);
      waitForStateTransfer(6, c1, c2, c3, c4);

      // Unblock the command with the new topology id from c1 on c3.
      // NonTxConcurrentDistributionInterceptor on c3 will throw an OutdatedTopologyException,
      // and StateTransferInterceptor on c1 will retry the command.
      // The DelayInterceptor on c1 will then block, waiting for an unblock() call.
      di3.unblock(1);

      // Unblock the command with the new topology id on c1
      log.tracef("Retrying the command on %s a second time", c1);
      di1.unblock(3);

      // Check that c2, c3 and c4 receive the forwarded command (no exceptions thrown).
      di2.unblock(3);
      di3.unblock(2);
      di4.unblock(1);

      log.tracef("Waiting for the put command to finish on %s", c1);
      Object retval = f.get(10, TimeUnit.SECONDS);
      log.tracef("Put command finished on %s", c1);

      assertNull(retval);

      // 1 direct invocation + 2 retries
      assertEquals(3, di1.getCounter());
      // 1 from each invocation/retry on c1
      assertEquals(3, di2.getCounter());
      // 1 for each invocation/retry on c1, minus the first invocation in which c3 wasn't started
      assertEquals(2, di3.getCounter());
      // just the last retry on c1
      assertEquals(1, di4.getCounter());
   }

   public void testForwardToJoinerTransactional() throws Exception {
      EmbeddedCacheManager cm1 = addClusterEnabledCacheManager(buildConfig(true));
      final Cache<Object, Object> c1 = cm1.getCache();
      DelayInterceptor di1 = findInterceptor(c1, DelayInterceptor.class);

      EmbeddedCacheManager cm2 = addClusterEnabledCacheManager(buildConfig(true));
      Cache c2 = cm2.getCache();
      DelayInterceptor di2 = findInterceptor(c2, DelayInterceptor.class);
      waitForStateTransfer(2, c1, c2);

      Future<Object> f = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            log.tracef("Initiating a transaction on %s", c1);
            // The prepare command is replicated to cache c2, and it blocks in the DelayInterceptor.
            c1.put("k", "v");
            return null;
         }
      });

      // c3 joins, topology id changes
      EmbeddedCacheManager cm3 = addClusterEnabledCacheManager(buildConfig(true));
      Cache c3 = cm3.getCache();
      DelayInterceptor di3 = findInterceptor(c3, DelayInterceptor.class);
      waitForStateTransfer(4, c1, c2, c3);

      // Unblock the replicated command on c2.
      // StateTransferInterceptor will forward the command to c3.
      // The DelayInterceptor on c3 will then block, waiting for an unblock() call.
      log.tracef("Forwarding the prepare command from %s", c2);
      di2.unblock(1);

      // c4 joins, topology id changes
      EmbeddedCacheManager cm4 = addClusterEnabledCacheManager(buildConfig(true));
      Cache c4 = cm4.getCache();
      DelayInterceptor di4 = findInterceptor(c4, DelayInterceptor.class);
      waitForStateTransfer(6, c1, c2, c3, c4);

      // Unblock the forwarded command on c3.
      // StateTransferInterceptor will then forward the command to c2 and c4.
      log.tracef("Forwarding the prepare command from %s", c3);
      di3.unblock(1);

      // Check that the c2 and c4 received the forwarded command.
      di2.unblock(2);
      di4.unblock(1);

      // Allow the command to proceed on the originator (c1).
      // StateTransferInterceptor will forward the command to c2, c3, and c4.
      log.tracef("Forwarding the prepare command from %s", c1);
      di1.unblock(1);

      // Check that c2, c3, and c4 received the forwarded command.
      di2.unblock(3);
      di3.unblock(2);
      di4.unblock(2);

      log.tracef("Waiting for the transaction to finish on %s", c1);
      f.get(10, TimeUnit.SECONDS);
      log.tracef("Transaction finished on %s", c1);

      assertEquals(di1.getCounter(), 1);
      // 1 from replication + 1 re-forwarded by C + 1 forwarded by A
      assertEquals(di2.getCounter(), 3);
      // 1 forwarded by B + 1 forwarded by A
      assertEquals(di3.getCounter(), 2);
      // 1 re-1forwarded by C + 1 forwarded by A
      assertEquals(di4.getCounter(), 2);
   }

   private void waitForStateTransfer(int expectedTopologyId, Cache... caches) {
      waitForRehashToComplete(caches);
      for (Cache c : caches) {
         CacheTopology cacheTopology = extractComponent(c, StateTransferManager.class).getCacheTopology();
         assertEquals(cacheTopology.getTopologyId(), expectedTopologyId,
               String.format("Wrong topology on cache %s, expected %d and got %s",
                     c, expectedTopologyId, cacheTopology));
      }
   }

   private class DelayInterceptor extends BaseCustomInterceptor {
      private final AtomicInteger counter = new AtomicInteger(0);
      private final SynchronousQueue<Object> barrier = new SynchronousQueue<Object>(true);

      public int getCounter() {
         return counter.get();
      }

      public void unblock(int count) throws InterruptedException, TimeoutException, BrokenBarrierException {
         log.tracef("Unblocking command on cache %s", cache);
         boolean offerResult = barrier.offer(count, 5, TimeUnit.SECONDS);
         assertTrue(offerResult, String.format("DelayInterceptor of cache %s is not waiting to be unblocked", cache));
      }

      @Override
      public Object visitPutKeyValueCommand(InvocationContext ctx, PutKeyValueCommand command) throws Throwable {
         if (!ctx.isInTxScope() && !command.hasFlag(Flag.PUT_FOR_STATE_TRANSFER)) {
            log.tracef("Delaying command %s originating from %s", command, ctx.getOrigin());
            Integer myCount = counter.incrementAndGet();
            Object pollResult = barrier.poll(15, TimeUnit.SECONDS);
            assertEquals(pollResult, myCount,
                  String.format("Timed out waiting for unblock(%d) call on cache %s", myCount, cache));
            log.tracef("Command unblocked: %s", command);
         }

         Object result = super.visitPutKeyValueCommand(ctx, command);
         return result;
      }

      @Override
      public Object visitPrepareCommand(TxInvocationContext ctx, PrepareCommand command) throws Throwable {
         Object result = super.visitPrepareCommand(ctx, command);
         if (!ctx.isOriginLocal() || !((LocalTransaction)ctx.getCacheTransaction()).isFromStateTransfer()) {
            log.tracef("Delaying command %s originating from %s", command, ctx.getOrigin());
            Integer myCount = counter.incrementAndGet();
            Object pollResult = barrier.poll(15, TimeUnit.SECONDS);
            assertEquals(pollResult, myCount,
                  String.format("Timed out waiting for unblock(%d) call on cache %s", myCount, cache));
            log.tracef("Command unblocked: %s", command);
         }
         return result;
      }

      @Override
      public String toString() {
         return "DelayInterceptor{counter=" + counter + "}";
      }
   }
}
