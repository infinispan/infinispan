package org.infinispan.distribution.rehash;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.VisitableCommand;
import org.infinispan.commands.write.PutKeyValueCommand;
import org.infinispan.commands.write.RemoveCommand;
import org.infinispan.commands.write.ReplaceCommand;
import org.infinispan.commons.api.BasicCacheContainer;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.container.DataContainer;
import org.infinispan.distribution.BlockingInterceptor;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.interceptors.EntryWrappingInterceptor;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.rpc.RpcManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CheckPoint;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.topology.ClusterTopologyManager;
import org.infinispan.transaction.TransactionMode;
import org.infinispan.tx.dld.ControlledRpcManager;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * Tests that state transfer can't overwrite a value written by a command during state transfer.
 * See https://issues.jboss.org/browse/ISPN-3443
 *
 * @author Dan Berindei
 */
@Test(groups = "functional", testName = "distribution.rehash.NonTxStateTransferOverwritingValueTest")
@CleanupAfterMethod
public class NonTxStateTransferOverwritingValueTest extends MultipleCacheManagersTest {

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
               return cache0.put(key, value);
            case PUT_IF_ABSENT:
               return cache0.putIfAbsent(key, value);
            case REPLACE:
               return cache0.replace(key, value);
            case REPLACE_EXACT:
               return cache0.replace(key, previousValue, value);
            case REMOVE:
               return cache0.remove(key);
            case REMOVE_EXACT:
               return cache0.remove(key, previousValue);
            default:
               throw new IllegalArgumentException("Unsupported operation: " + this);
         }
      }
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder c = getConfigurationBuilder();

      addClusterEnabledCacheManager(c);
      waitForClusterToForm();
   }

   protected ConfigurationBuilder getConfigurationBuilder() {
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
      final MagicKey key = new MagicKey(cache0);

      // Prepare for replace/remove: put a previous value in cache0
      final Object previousValue = op.getPreviousValue();
      if (previousValue != null) {
         cache0.put(key, previousValue);
         assertEquals(previousValue, cache0.get(key));
         log.tracef("Previous value inserted: %s = %s", key, previousValue);
      }

      int preJoinTopologyId = cache0.getComponentRegistry().getStateTransferManager().getCacheTopology().getTopologyId();

      // Block any state response commands on cache0
      CheckPoint checkPoint = new CheckPoint();
      ControlledRpcManager blockingRpcManager0 = blockStateResponseCommand(cache0);

      // Block the rebalance confirmation on cache0
      blockRebalanceConfirmation(manager(0), checkPoint);

      // Start the joiner
      log.tracef("Starting the cache on the joiner");
      ConfigurationBuilder c = getConfigurationBuilder();
      c.clustering().stateTransfer().awaitInitialTransfer(false);
      addClusterEnabledCacheManager(c);

      final AdvancedCache<Object,Object> cache1 = advancedCache(1);
      int rebalanceTopologyId = preJoinTopologyId + 1;

      // Wait for the write CH to contain the joiner everywhere
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return cache0.getRpcManager().getMembers().size() == 2 &&
                  cache1.getRpcManager().getMembers().size() == 2;
         }
      });

      // Every PutKeyValueCommand will be blocked before committing the entry on cache1
      CyclicBarrier beforeCommitCache1Barrier = new CyclicBarrier(2);
      BlockingInterceptor blockingInterceptor1 = new BlockingInterceptor(beforeCommitCache1Barrier,
            op.getCommandClass(), true);
      cache1.addInterceptorAfter(blockingInterceptor1, EntryWrappingInterceptor.class);

      // Wait for cache0 to collect the state to send to cache1 (including our previous value).
      blockingRpcManager0.waitForCommandToBlock();

      // Put/Replace/Remove from cache0 with cache0 as primary owner, cache1 will become a backup owner for the retry
      // The put command will be blocked on cache1 just before committing the entry.
      Future<Object> future = fork(new Callable<Object>() {
         @Override
         public Object call() throws Exception {
            return op.perform(cache0, key);
         }
      });

      // Wait for the entry to be wrapped on cache1
      beforeCommitCache1Barrier.await(10, TimeUnit.SECONDS);

      // Allow the state to be applied on cache1 (writing the old value for our entry)
      blockingRpcManager0.stopBlocking();

      // Wait for cache1 to finish applying the state, but don't allow the rebalance confirmation to be processed.
      // (It would change the topology and it would trigger a retry for the command.)
      checkPoint.awaitStrict("pre_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(1), 10, SECONDS);

      // Now allow the command to commit on cache1
      beforeCommitCache1Barrier.await(10, TimeUnit.SECONDS);

      // Wait for the command to finish and check that it didn't fail
      Object result = future.get(10, TimeUnit.SECONDS);
      assertEquals(op.getReturnValue(), result);
      log.tracef("%s operation is done", op);

      // Allow the rebalance confirmation to proceed and wait for the topology to change everywhere
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(0));
      checkPoint.trigger("resume_rebalance_confirmation_" + rebalanceTopologyId + "_from_" + address(1));
      TestingUtil.waitForRehashToComplete(cache0, cache1);

      // Check the value on all the nodes
      assertEquals(op.getValue(), cache0.get(key));
      assertEquals(op.getValue(), cache1.get(key));
   }

   private ControlledRpcManager blockStateResponseCommand(final Cache cache) throws InterruptedException {
      RpcManager rpcManager = TestingUtil.extractComponent(cache, RpcManager.class);
      ControlledRpcManager controlledRpcManager = new ControlledRpcManager(rpcManager);
      controlledRpcManager.blockBefore(StateResponseCommand.class);
      TestingUtil.replaceComponent(cache, RpcManager.class, controlledRpcManager, true);
      return controlledRpcManager;
   }

   private void blockRebalanceConfirmation(final EmbeddedCacheManager manager, final CheckPoint checkPoint)
         throws Exception {
      ClusterTopologyManager ctm = TestingUtil.extractGlobalComponent(manager, ClusterTopologyManager.class);
      ClusterTopologyManager spyManager = spy(ctm);
      TestingUtil.replaceComponent(manager, ClusterTopologyManager.class, spyManager, true);
      doAnswer(new Answer<Object>() {
         @Override
         public Object answer(InvocationOnMock invocation) throws Throwable {
            Object[] arguments = invocation.getArguments();
            Address source = (Address) arguments[1];
            int topologyId = (Integer) arguments[2];
            checkPoint.trigger("pre_rebalance_confirmation_" + topologyId + "_from_" + source);
            checkPoint.awaitStrict("resume_rebalance_confirmation_" + topologyId + "_from_" + source, 10, SECONDS);
            return invocation.callRealMethod();
         }
      }).when(spyManager).handleRebalanceCompleted(anyString(), any(Address.class), anyInt(), any(Throwable.class),
            anyInt());
   }
}