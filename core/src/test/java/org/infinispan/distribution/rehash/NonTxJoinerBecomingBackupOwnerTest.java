package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.read.GetKeyValueCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.context.Flag;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.interceptors.distribution.NonTxDistributionInterceptor;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.testng.AssertJUnit.assertEquals;

/**
 * Test that a joiner that became a backup owner for a key does not check the previous value when doing a conditional
 * write. Also check that if executing a write command during state transfer, it doesn't perform a remote get to obtain
 * the previous value from one of the readCH owners.
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxJoinerBecomingBackupOwnerTest")
@CleanupAfterMethod
public class NonTxJoinerBecomingBackupOwnerTest extends MultipleCacheManagersTest {

   private static final String CACHE_NAME = BasicCacheContainer.DEFAULT_CACHE_NAME;
   private static enum Operation {
      PUT(PutKeyValueCommand.class, "v1", null, null),
      PUT_IF_ABSENT(PutKeyValueCommand.class, "v1", null, null),
      REPLACE(ReplaceCommand.class, "v1", "v0", "v0"),
      REPLACE_EXACT(ReplaceCommand.class, "v1", "v0", true),
      REMOVE(RemoveCommand.class, null, "v0", "v0"),
      REMOVE_EXACT(RemoveCommand.class, null, "v0", true);

      private final Class<? extends VisitableCommand> commandClass;
      private final Object value;
      private final Object previousValue;
      private final Object returnValue;

      Operation(Class<? extends VisitableCommand> commandClass, Object value, Object previousValue, 
                Object returnValue) {
         this.commandClass = commandClass;
         this.value = value;
         this.previousValue = previousValue;
         this.returnValue = returnValue;
      }

      private Class<? extends VisitableCommand> getCommandClass() {
         return commandClass;
      }

      private Object getValue() {
         return value;
      }

      private Object getPreviousValue() {
         return previousValue;
      }

      private Object getReturnValue() {
         return returnValue;
      }

      private Object perform(AdvancedCache<Object, Object> cache0, MagicKey key) {
         switch (this) {
            case PUT:
               return cache0.put(key, getValue());
            case PUT_IF_ABSENT:
               return cache0.putIfAbsent(key, getValue());
            case REPLACE:
               return cache0.replace(key, getValue());
            case REPLACE_EXACT:
               return cache0.replace(key, getPreviousValue(), getValue());
            case REMOVE:
               return cache0.remove(key);
            case REMOVE_EXACT:
               return cache0.remove(key, getPreviousValue());
            default:
               throw new IllegalArgumentException("Unsupported operation: " + this);
         }
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   private ConfigurationBuilder getConfigurationBuilder() {
      ConfigurationBuilder c = new ConfigurationBuilder();
      c.clustering().cacheMode(CacheMode.DIST_SYNC);
      c.transaction().transactionMode(TransactionMode.NON_TRANSACTIONAL);
      return c;
   }

   public void testBackupOwnerJoiningDuringPut() throws Exception {
      doTest(Operation.PUT);
   }

   public void testBackupOwnerJoiningDuringPutIfAbsent() throws Exception {
      doTest(Operation.PUT_IF_ABSENT);
   }

   public void testBackupOwnerJoiningDuringReplace() throws Exception {
      doTest(Operation.REPLACE);
   }

   public void testBackupOwnerJoiningDuringReplaceWithPreviousValue() throws Exception {
      doTest(Operation.REPLACE_EXACT);
   }

   public void testBackupOwnerJoiningDuringRemove() throws Exception {
      doTest(Operation.REMOVE);
   }

   public void testBackupOwnerJoiningDuringRemoveWithPreviousValue() throws Exception {
      doTest(Operation.REMOVE_EXACT);
   }

   private void doTest(final Operation op) throws Exception {
      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      final AdvancedCache<Object, Object> cache1 = advancedCache(1);

      // Install a ControlledRpcManager on cache1 so that we know when it finished sending the state
      ControlledRpcManager blockingRpcManager1 = blockStateResponseCommand(cache1);

      // Add a new member, but don't start the cache yet
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);

      // Start the cache and wait until it's a member in the write CH
      log.tracef("Starting the cache on the joiner");
      final AdvancedCache<Object,Object> cache2 = advancedCache(2);

      // Wait for the write CH to contain the joiner everywhere
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getRpcManager().getMembers().size() == 3 &&
                  cache1.getRpcManager().getMembers().size() == 3 &&
                  cache2.getRpcManager().getMembers().size() == 3;
         }
      });

      // Every ClusteredGetKeyValueCommand will be blocked before returning on cache0
      CyclicBarrier beforeCache0Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor0 = new BlockingInterceptor(beforeCache0Barrier,
            GetKeyValueCommand.class, false);
      cache0.addInterceptorBefore(blockingInterceptor0, StateTransferInterceptor.class);

      // Every PutKeyValueCommand will be blocked before returning on cache1
      CyclicBarrier afterCache1Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor1 = new BlockingInterceptor(afterCache1Barrier,
            op.getCommandClass(), false);
      cache1.addInterceptorBefore(blockingInterceptor1, StateTransferInterceptor.class);

      // Every PutKeyValueCommand will be blocked before reaching the distribution interceptor on cache2
      CyclicBarrier beforeCache2Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor2 = new BlockingInterceptor(beforeCache2Barrier,
            op.getCommandClass(), true);
      cache2.addInterceptorBefore(blockingInterceptor2, NonTxDistributionInterceptor.class);

      // Wait for cache1 to send the StateResponseCommand to cache1, but keep it blocked
      // We only block the StateResponseCommand on cache1, because that's the node cache2 will ask for the magic key
      blockingRpcManager1.waitForCommandToBlock();

      final MagicKey key = getKeyForCache2();

      // Prepare for replace: put a previous value in cache0 and cache1
      if (op.getPreviousValue() != null) {
         cache0.withFlags(Flag.CACHE_MODE_LOCAL).put(key, op.getPreviousValue());
         cache1.withFlags(Flag.CACHE_MODE_LOCAL).put(key, op.getPreviousValue());
      }

      // Put from cache0 with cache0 as primary owner, cache2 will become a backup owner for the retry
      // The put command will be blocked on cache1 and cache2.
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return op.perform(cache0, key);
         }
      });

      // Wait for the value to be written on cache1
      afterCache1Barrier.await(10, TimeUnit.SECONDS);
      afterCache1Barrier.await(10, TimeUnit.SECONDS);

      // Allow the command to proceed on cache2
      beforeCache2Barrier.await(10, TimeUnit.SECONDS);
      beforeCache2Barrier.await(10, TimeUnit.SECONDS);

      // Check that the put command didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("%s operation is done", op);

      // Stop blocking get commands on cache0
//      beforeCache0Barrier.await(10, TimeUnit.SECONDS);
//      beforeCache0Barrier.await(10, TimeUnit.SECONDS);
      cache0.removeInterceptor(BlockingInterceptor.class);

      // Allow cache2 to receive the StateResponseCommand from cache1 and the cluster to finish state transfer
      blockingRpcManager1.stopBlocking();

      // Wait for the topology to change everywhere
      TestingUtil.waitForRehashToComplete(cache0, cache1, cache2);

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));
      assertEquals(op.getValue(), cache2.get(key));

   }

   private MagicKey getKeyForCache2() {
      return new MagicKey(cache(0), cache(1), cache(2));
   }

   private ControlledRpcManager blockStateResponseCommand(final Cache cache) throws InterruptedException {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager);
      controlledRpcManager.blockBefore(StateResponseCommand.class);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }
}