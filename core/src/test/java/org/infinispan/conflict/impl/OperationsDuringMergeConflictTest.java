package org.infinispan.conflict.impl;

import org.infinispan.AdvancedCache;
import org.infinispan.Cache;
import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.commands.topology.TopologyUpdateCommand;
import org.infinispan.distribution.MagicKey;
import org.infinispan.remoting.inboundhandler.ControllingInboundInvocationHandler;
import org.infinispan.remoting.inboundhandler.ControllingPerCacheInboundInvocationHandler;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestInternalCacheEntryFactory;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.Test;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.infinispan.configuration.cache.CacheMode.DIST_SYNC;
import static org.infinispan.topology.CacheTopology.Phase.READ_OLD_WRITE_ALL;

/**
 * 1. do a split, and let k -> A, k -> B in the two partitions
 * 2. initiate a conflict resolution, with merge policy saying that merge A,B = C
 * 3. check that members from each partition read A (in p1) or B (in p2)
 * 4. let someone from p1 issue a write k -> D, check that both p1 and p2 now reads D
 * 5. let the actual merge proceed (be ignored)
 * 6. check that all nodes still read D
 * 7. let state transfer proceed and check that D is still in
 * <p>
 * For sanity check, you should be able to disable the write of D and see C everywhere instead.
 * And the same should work for removal as well (merge should not overwrite removal), though I think that CommitManager will behave in the same way.
 *
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
      return new Object[]{
            new OperationsDuringMergeConflictTest(MergeAction.NONE),
            new OperationsDuringMergeConflictTest(MergeAction.PUT),
            new OperationsDuringMergeConflictTest(MergeAction.REMOVE)
      };
   }

   private MergeAction mergeAction;

   public OperationsDuringMergeConflictTest() {
   }

   public OperationsDuringMergeConflictTest(MergeAction mergeAction) {
      super(DIST_SYNC, null, new int[]{0, 1}, new int[]{2, 3});
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
      List<ControllingInboundInvocationHandler> inboundHandlers = IntStream.range(0, numMembersInCluster)
            .mapToObj(i -> ControllingInboundInvocationHandler.replace(manager(i)))
            .collect(Collectors.toList());
      List<ControllingPerCacheInboundInvocationHandler> perCacheInboundInvocationHandlers = IntStream.range(0, numMembersInCluster)
            .mapToObj(i -> ControllingPerCacheInboundInvocationHandler.replace(cache(i)))
            .collect(Collectors.toList());

      inboundHandlers.forEach(handler -> handler.blockRpcBefore(cmd -> cmd instanceof TopologyUpdateCommand && ((TopologyUpdateCommand) cmd).getPhase() == READ_OLD_WRITE_ALL));
      perCacheInboundInvocationHandlers.forEach(handler -> handler.blockRpcBefore(StateResponseCommand.class));

      try {
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
         inboundHandlers.forEach(ControllingInboundInvocationHandler::stopBlocking);
         perCacheInboundInvocationHandlers.forEach(ControllingPerCacheInboundInvocationHandler::stopBlocking);
         TestingUtil.waitForNoRebalance(caches());
         assertCacheGetValAllCaches(mergeAction);
      } catch (Throwable t) {
         inboundHandlers.forEach(ControllingInboundInvocationHandler::stopBlocking);
         perCacheInboundInvocationHandlers.forEach(ControllingPerCacheInboundInvocationHandler::stopBlocking);
         throw t;
      }
   }

   private void assertCacheGetValAllCaches(MergeAction action) {
      assertCacheGet(conflictKey, action.value, cacheIndexes());
   }

}
