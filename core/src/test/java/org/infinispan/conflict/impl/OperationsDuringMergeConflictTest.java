package org.infinispan.conflict.impl;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.test.TestingUtil.extractGlobalComponent;
import static org.infinispan.test.TestingUtil.replaceComponent;
import static org.infinispan.test.TestingUtil.wrapInboundInvocationHandler;
import static org.infinispan.topology.CacheTopology.Phase.READ_OLD_WRITE_ALL;
import static org.testng.AssertJUnit.fail;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.ReplicableCommand;
import org.infinispan.commands.remote.CacheRpcCommand;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.remoting.inboundhandler.AbstractDelegatingHandler;
import org.infinispan.remoting.inboundhandler.DeliverOrder;
import org.infinispan.remoting.inboundhandler.InboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.PerCacheInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.Reply;
import org.infinispan.remoting.transport.Address;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.topology.CacheTopology;
import org.infinispan.topology.CacheTopologyControlCommand;
import org.infinispan.xsite.XSiteReplicateCommand;
import org.testng.annotations.Test;

/**
 1. do a split, and let k -> A, k -> B in the two partitions
 2. initiate a conflict resolution, with merge policy saying that merge A,B = C
 3. check that members from each partition read A (in p1) or B (in p2)
 4. let someone from p1 issue a write k -> D, check that both p1 and p2 now reads D
 5. let the actual merge proceed (be ignored)
 6. check that all nodes still read D
 7. let state transfer proceed and check that D is still in

 For sanity check, you should be able to disable the write of D and see C everywhere instead.
 And the same should work for removal as well (merge should not overwrite removal), though I think that CommitManager will behave in the same way.
 * @author Ryan Emerson
 * @since 9.1
 */
@Test(groups = "functional", testName = "conflict.impl.OperationsDuringMergeConflictTest")
public class OperationsDuringMergeConflictTest extends BaseMergePolicyTest {

   private static final String PARTITION_0_VAL = "A";
   private static final String PARTITION_1_VAL = "B";
   private static final String MERGE_RESULT = "C";
   private static final String PUT_RESULT = "D";

   private enum MergeAction {
      PUT(PUT_RESULT),
      REMOVE(null),
      NONE(MERGE_RESULT);

      String value;
      MergeAction(String value) {
         this.value = value;
      }
   }

   @Override
   public Object[] factory() {
      return new Object[] {
            new OperationsDuringMergeConflictTest(MergeAction.NONE),
            new OperationsDuringMergeConflictTest(MergeAction.PUT),
            new OperationsDuringMergeConflictTest(MergeAction.REMOVE)
      };
   }

   private MergeAction mergeAction;

   public OperationsDuringMergeConflictTest(){}

   public OperationsDuringMergeConflictTest(MergeAction mergeAction) {
      super(DIST_SYNC, null, new int[]{0,1}, new int[]{2,3});
      this.mergePolicy = ((preferredEntry, otherEntries) -> TestInternalCacheEntryFactory.create(conflictKey, MERGE_RESULT));
      this.description = mergeAction.toString();
      this.mergeAction = mergeAction;
      this.valueAfterMerge = mergeAction.value;
   }

   @Override
   protected void beforeSplit() {
      conflictKey = new MagicKey(cache(p0.node(0)), cache(p1.node(0)));
   }

   @Override
   protected void duringSplit(AdvancedCache preferredPartitionCache, AdvancedCache otherCache) {
      cache(p0.node(0)).put(conflictKey, PARTITION_0_VAL);
      cache(p1.node(0)).put(conflictKey, PARTITION_1_VAL);

      assertCacheGet(conflictKey, PARTITION_0_VAL, p0.getNodes());
      assertCacheGet(conflictKey, PARTITION_1_VAL, p1.getNodes());
   }

   @Override
   protected void performMerge() {
      boolean modifyDuringMerge = mergeAction != MergeAction.NONE;
      CountDownLatch conflictLatch = new CountDownLatch(1);
      CountDownLatch stateTransferLatch = new CountDownLatch(1);
      try {
         IntStream.range(0, numMembersInCluster).forEach(i -> {
            wrapInboundInvocationHandler(cache(i), handler -> new BlockStateResponseCommandHandler(handler, conflictLatch));
            EmbeddedCacheManager manager = manager(i);
            InboundInvocationHandler handler = extractGlobalComponent(manager, InboundInvocationHandler.class);
            BlockingInboundInvocationHandler ourHandler = new BlockingInboundInvocationHandler(handler, stateTransferLatch);
            replaceComponent(manager, InboundInvocationHandler.class, ourHandler, true);
         });

         assertCacheGet(conflictKey, PARTITION_0_VAL, p0.getNodes());
         assertCacheGet(conflictKey, PARTITION_1_VAL, p1.getNodes());
         partition(0).merge(partition(1), false);
         assertCacheGet(conflictKey, PARTITION_0_VAL, p0.getNodes());
         assertCacheGet(conflictKey, PARTITION_1_VAL, p1.getNodes());

         if (modifyDuringMerge) {
            // Wait for CONFLICT_RESOLUTION topology to have been installed by the coordinator and then proceed
            List<Address> allMembers = caches().stream().map(cache -> cache.getCacheManager().getAddress()).collect(Collectors.toList());
            TestingUtil.waitForTopologyPhase(allMembers, CacheTopology.Phase.CONFLICT_RESOLUTION, caches().toArray(new Cache[numMembersInCluster]));
            if (mergeAction == MergeAction.PUT) {
               cache(0).put(conflictKey, mergeAction.value);
            } else {
               cache(0).remove(conflictKey);
            }
         }
         conflictLatch.countDown();
         stateTransferLatch.countDown();
         TestingUtil.waitForNoRebalance(caches());
         assertCacheGetValAllCaches(mergeAction);
      } catch (Throwable t) {
         conflictLatch.countDown();
         stateTransferLatch.countDown();
         throw t;
      }
   }

   private void assertCacheGetValAllCaches(MergeAction action) {
      assertCacheGet(conflictKey, action.value, cacheIndexes());
   }

   private class BlockingInboundInvocationHandler implements InboundInvocationHandler {
      final InboundInvocationHandler delegate;
      final CountDownLatch latch;

      BlockingInboundInvocationHandler(InboundInvocationHandler delegate, CountDownLatch latch) {
         this.delegate = delegate;
         this.latch = latch;
      }

      @Override
      public void handleFromCluster(Address origin, ReplicableCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof CacheTopologyControlCommand) {
            CacheTopologyControlCommand cmd = (CacheTopologyControlCommand) command;
            if (cmd.getType() == CacheTopologyControlCommand.Type.CH_UPDATE && cmd.getPhase() == READ_OLD_WRITE_ALL)
               awaitLatch(latch);
         }
         delegate.handleFromCluster(origin, command, reply, order);
      }

      @Override
      public void handleFromRemoteSite(String origin, XSiteReplicateCommand command, Reply reply, DeliverOrder order) {
         delegate.handleFromRemoteSite(origin, command, reply, order);
      }
   }

   private class BlockStateResponseCommandHandler extends AbstractDelegatingHandler {
      final CountDownLatch latch;

      BlockStateResponseCommandHandler(PerCacheInboundInvocationHandler delegate, CountDownLatch latch) {
         super(delegate);
         this.latch = latch;
      }

      @Override
      public void handle(CacheRpcCommand command, Reply reply, DeliverOrder order) {
         if (command instanceof StateResponseCommand)
            awaitLatch(latch);
         delegate.handle(command, reply, order);
      }
   }

   private void awaitLatch(CountDownLatch latch) {
      try {
         // Timeout has to be large enough to allow for rebalance and subsequent operations, so we double the
         // rebalance timeout. Timeout necessary as for some reason the latch is not always counted down in the Handler
         if (!latch.await(120, TimeUnit.SECONDS))
            fail("CountDownLatch await timedout");
      } catch (InterruptedException ignore) {
         fail("CountDownLatch Interrupted");
      }
   }
}
