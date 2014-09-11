package org.infinispan.partitionhandling;

import org.infinispan.commands.ReplicableCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partionhandling.AvailabilityException;
import org.infinispan.partionhandling.impl.AvailabilityMode;
import org.infinispan.partionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.statetransfer.StateTransferManager;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.CommandMatcher;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.test.concurrent.StateSequencerUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.Channel;
import org.jgroups.View;
import org.jgroups.protocols.pbcast.GMS;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * With a cluster made out of nodes {A,B,C,D}, tests that D stops gracefully and before the state transfer finishes,
 * another node C also stops. {A,B} should enter unavailable mode.
 * The only way in which it could recover is explicitly, through JMX operations.
 */
@Test(groups = "functional", testName = "partitionhandling.NumOwnersNodeCrashInSequenceTest")
@CleanupAfterMethod
public class NumOwnersNodeStopInSequenceTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(NumOwnersNodeStopInSequenceTest.class);

   ControlledConsistentHashFactory cchf;
   private ConfigurationBuilder configBuilder;

   @Override
   protected void createCacheManagers() throws Throwable {
      cchf = new ControlledConsistentHashFactory(new int[]{0, 1}, new int[]{1, 2},
                                            new int[]{2, 3}, new int[]{3, 0});
      configBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configBuilder.clustering().partitionHandling().enabled(true);
      configBuilder.clustering().hash().numSegments(4).stateTransfer().timeout(2000);
   }

   public void testNodeCrashedBeforeStFinished0() throws Exception {
      testNodeCrashedBeforeStFinished(0, 1, 2, 3);
   }

   public void testNodeCrashedBeforeStFinished1() throws Exception {
      testNodeCrashedBeforeStFinished(0, 2, 1, 3);
   }

   public void testNodeCrashedBeforeStFinished2() throws Exception {
      testNodeCrashedBeforeStFinished(0, 3, 1, 2);
   }

   public void testNodeCrashedBeforeStFinished3() throws Exception {
      testNodeCrashedBeforeStFinished(1, 2, 0, 3);
   }

   public void testNodeCrashedBeforeStFinished4() throws Exception {
      testNodeCrashedBeforeStFinished(1, 3, 0, 2);
   }

   public void testNodeCrashedBeforeStFinished5() throws Exception {
      testNodeCrashedBeforeStFinished(2, 3, 0, 1);
   }

   public void testNodeCrashedBeforeStFinished6() throws Exception {
      testNodeCrashedBeforeStFinished(1, 2, 3, 0);
   }

   public void testNodeCrashedBeforeStFinished7() throws Exception {
      testNodeCrashedBeforeStFinished(2, 3, 1, 0);
   }


   private void testNodeCrashedBeforeStFinished(final int a0, final int a1, final int c0, final int c1) throws Exception {

      cchf.setOwnerIndexes(new int[]{a0, a1}, new int[]{a1, c0},
                           new int[]{c0, c1}, new int[]{c1, a0});
      configBuilder.clustering().hash().consistentHashFactory(cchf);
      createCluster(configBuilder, 4);
      waitForClusterToForm();



      Object k0 = new MagicKey("k1", cache(a0), cache(a1));
      Object k1 = new MagicKey("k2", cache(a0), cache(a1));
      Object k2 = new MagicKey("k3", cache(a1), cache(c0));
      Object k3 = new MagicKey("k4", cache(a1), cache(c0));
      Object k4 = new MagicKey("k5", cache(c0), cache(c1));
      Object k5 = new MagicKey("k6", cache(c0), cache(c1));
      Object k6 = new MagicKey("k7", cache(c1), cache(a0));
      Object k7 = new MagicKey("k8", cache(c1), cache(a0));

      final Object[] allKeys = new Object[] {k0, k1, k2, k3, k4, k5, k6, k7};
      for (Object k : allKeys) cache(a0).put(k, k);

      StateSequencer ss = new StateSequencer();
      ss.logicalThread("main", "main:st_in_progress", "main:2nd_node_left", "main:cluster_unavailable");

      final StateTransferManager stm0 = advancedCache(a0).getComponentRegistry().getStateTransferManager();
      final int initialTopologyId = stm0.getCacheTopology().getTopologyId();
      StateSequencerUtil.advanceOnInboundRpc(ss, manager(a1), new CommandMatcher() {
         @Override
         public boolean accept(ReplicableCommand command) {
            if (!(command instanceof StateResponseCommand))
               return false;
            StateResponseCommand responseCommand = (StateResponseCommand) command;
            return initialTopologyId < responseCommand.getCommandId();
         }
      }).before("main:st_in_progress", "main:cluster_unavailable");

      // Prepare for rebalance. Manager a1 will request state from c0 for segment 2
      cchf.setMembersToUse(advancedCache(a0).getRpcManager().getTransport().getMembers());
      cchf.setOwnerIndexes(new int[]{a0, a1}, new int[]{a1, c0},
            new int[]{c0, a1}, new int[]{c0, a0});

      Address missing = address(c1);
      log.tracef("Before killing node %s", missing);
      TestingUtil.killCacheManagers(manager(c1));
      installNewView(advancedCache(a0).getRpcManager().getTransport().getMembers(), missing, manager(a0), manager(a1), manager(c0));

      ss.enter("main:2nd_node_left");

      missing = address(c0);
      log.tracef("Killing 2nd node %s", missing);
      TestingUtil.killCacheManagers(manager(c0));
      installNewView(advancedCache(a0).getRpcManager().getTransport().getMembers(), missing, manager(a0), manager(a1));

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            PartitionHandlingManager phm0 = TestingUtil.extractComponent(cache(a0), PartitionHandlingManager.class);
            return phm0.getAvailabilityMode() == AvailabilityMode.UNAVAILABLE;
         }
      });
      ss.exit("main:2nd_node_left");

      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            log.trace("Testing condition");
            for (Object k : allKeys) {
               try {
                  cache(a0).get(k);
                  return false;
               } catch (AvailabilityException e) {
               }
               try {
                  cache(a1).put(k, k);
                  return false;
               } catch (AvailabilityException e) {
               }
            }
            return true;
         }
      });
   }

   private void installNewView(List<Address> members, Address missing, EmbeddedCacheManager... where) {
      log.tracef("installNewView:members=%s, missing=%s", members, missing);
      final List<org.jgroups.Address> viewMembers = new ArrayList<org.jgroups.Address>();
      for (Address a : members)
         if (!a.equals(missing))
            viewMembers.add(((JGroupsAddress) a).getJGroupsAddress());
      int viewId = where[0].getTransport().getViewId() + 1;
      View view = View.create(viewMembers.get(0), viewId, (org.jgroups.Address[]) viewMembers.toArray(new org.jgroups.Address[viewMembers.size()]));

      log.trace("Before installing new view:" + viewMembers);
      for (EmbeddedCacheManager ecm : where) {
         Channel c = ((JGroupsTransport) ecm.getTransport()).getChannel();
         ((GMS) c.getProtocolStack().findProtocol(GMS.class)).installView(view);
      }
   }

}
