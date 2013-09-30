package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
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
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.statetransfer.StateTransferInterceptor;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

/**
 * Tests data loss during state transfer when the originator of a put operation becomes the primary owner of the
 * modified key. See https://issues.jboss.org/browse/ISPN-3357
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
      CheckPoint checkPoint = new CheckPoint();
      LocalTopologyManager ltm0 = TestingUtil.extractGlobalComponent(manager(0), LocalTopologyManager.class);
      int preJoinTopologyId = ltm0.getCacheTopology(CACHE_NAME).getTopologyId();

      final AdvancedCache<Object, Object> cache0 = advancedCache(0);
      addBlockingLocalTopologyManager(manager(0), checkPoint, preJoinTopologyId);

      final AdvancedCache<Object, Object> cache1 = advancedCache(1);
      addBlockingLocalTopologyManager(manager(1), checkPoint, preJoinTopologyId);

      // Add a new member, but don't start the cache yet
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);
      addBlockingLocalTopologyManager(manager(2), checkPoint, preJoinTopologyId);

      // Start the cache and wait until it's a member in the write CH
      log.tracef("Starting the cache on the joiner");
      final AdvancedCache<Object,Object> cache2 = advancedCache(2);
      int duringJoinTopologyId = preJoinTopologyId + 1;
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(0));
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + duringJoinTopologyId + "_on_" + address(2));

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

      // Allow the rebalance to end
      int postJoinTopologyId = duringJoinTopologyId + 1;
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(0));
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(1));
      checkPoint.trigger("allow_topology_" + postJoinTopologyId + "_on_" + address(2));

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

   private void addBlockingLocalTopologyManager(final EmbeddedCacheManager manager, final CheckPoint checkPoint,
                                                final int currentTopologyId)
         throws InterruptedException {
      LocalTopologyManager component = TestingUtil.extractGlobalComponent(manager, LocalTopologyManager.class);
      LocalTopologyManager spyLtm = Mockito.spy(component);
      doAnswer(new Answer() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            CacheTopology topology = (CacheTopology) invocation.getArguments()[1];
            // Ignore the first topology update on the joiner, which is with the topology before the join
            if (topology.getTopologyId() != currentTopologyId) {
               checkPoint.trigger("pre_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress());
               checkPoint.await("allow_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress(),
                     10, TimeUnit.SECONDS);
            }
            Object result = invocation.callRealMethod();
            checkPoint.trigger("post_topology_" + topology.getTopologyId() + "_on_" + manager.getAddress());
            return result;
         }
      }).when(spyLtm).handleConsistentHashUpdate(eq(CacheContainer.DEFAULT_CACHE_NAME), any(CacheTopology.class),
            anyInt());
      TestingUtil.extractGlobalComponentRegistry(manager).registerComponent(spyLtm, LocalTopologyManager.class);
   }
}