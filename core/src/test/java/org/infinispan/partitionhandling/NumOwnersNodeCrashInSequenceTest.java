package org.infinispan.partitionhandling;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.MagicKey;
import org.infinispan.distribution.ch.ConsistentHash;
import org.infinispan.manager.CacheContainer;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.remoting.transport.Transport;
import org.infinispan.remoting.transport.jgroups.JGroupsAddress;
import org.infinispan.remoting.transport.jgroups.JGroupsTransport;
import org.infinispan.statetransfer.StateResponseCommand;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.DISCARD;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.testng.annotations.Test;

/**
 * With a cluster made out of nodes {A,B,C,D}, tests that D crashes and before the state transfer finishes, another node
 * C crashes. {A,B} should enter in degraded mode. The only way in which it could recover is explicitly, through JMX
 * operations.
 */
@Test(groups = "functional", testName = "partitionhandling.NumOwnersNodeCrashInSequenceTest")
public class NumOwnersNodeCrashInSequenceTest extends MultipleCacheManagersTest {

   private static Log log = LogFactory.getLog(NumOwnersNodeCrashInSequenceTest.class);

   ControlledConsistentHashFactory cchf;
   private ConfigurationBuilder configBuilder;
   protected AvailabilityMode expectedAvailabilityMode;

   public NumOwnersNodeCrashInSequenceTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
      expectedAvailabilityMode = AvailabilityMode.DEGRADED_MODE;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      cchf = new ControlledConsistentHashFactory(new int[]{0, 1}, new int[]{1, 2},
                                            new int[]{2, 3}, new int[]{3, 0});
      configBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configBuilder.clustering().partitionHandling().enabled(true);
      configBuilder.clustering().hash().numSegments(4).stateTransfer().timeout(30000);
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
      ss.logicalThread("main", "main:st_in_progress", "main:2nd_node_left", "main:cluster_degraded", "main:after_cluster_degraded");

      advanceOnInboundRpc(ss, advancedCache(a1),
            matchCommand(StateResponseCommand.class).matchCount(0).build())
            .before("main:st_in_progress", "main:cluster_degraded");
      // When the coordinator node stops gracefully there are two rebalance operations, one with the old coord
      // and one with the new coord. The second
      advanceOnInboundRpc(ss, advancedCache(a1),
            matchCommand(StateResponseCommand.class).matchCount(1).build())
            .before("main:after_cluster_degraded");

      // Prepare for rebalance. Manager a1 will request state from c0 for segment 2
      cchf.setMembersToUse(advancedCache(a0).getRpcManager().getTransport().getMembers());
      cchf.setOwnerIndexes(new int[]{a0, a1}, new int[]{a1, c0},
            new int[]{c0, a1}, new int[]{c0, a0});

      Address missing = address(c1);
      log.tracef("Before killing node %s", missing);
      crashCacheManagers(manager(c1));
      installNewView(advancedCache(a0).getRpcManager().getTransport().getMembers(), missing, manager(a0), manager(a1)
            , manager(c0));

      ss.enter("main:2nd_node_left");

      missing = address(c0);
      log.tracef("Killing 2nd node %s", missing);
      crashCacheManagers(manager(c0));
      installNewView(advancedCache(a0).getRpcManager().getTransport().getMembers(), missing, manager(a0), manager(a1));

      final PartitionHandlingManager phm0 = TestingUtil.extractComponent(cache(a0), PartitionHandlingManager.class);
      final PartitionHandlingManager phm1 = TestingUtil.extractComponent(cache(a1), PartitionHandlingManager.class);
      eventually(new Condition() {
         @Override
         public boolean isSatisfied() throws Exception {
            return phm0.getAvailabilityMode() == expectedAvailabilityMode && phm1.getAvailabilityMode() == expectedAvailabilityMode;
         }
      });
      ss.exit("main:2nd_node_left");

      log.trace("Testing condition");
      ConsistentHash ch = cache(a0).getAdvancedCache().getDistributionManager().getReadConsistentHash();
      assertEquals(3, ch.getMembers().size());
      for (Object k : allKeys) {
         Collection<Address> owners = ch.locateOwners(k);
         try {
            cache(a0).get(k);
            if (owners.contains(address(c0)) || owners.contains(address(c1))) {
               fail("get(" + k + ") should have failed on cache " + address(a0));
            }
         } catch (AvailabilityException e) {
         }
         try {
            cache(a1).put(k, k);
            if (owners.contains(address(c0)) || owners.contains(address(c1))) {
               fail("put(" + k + ", v) should have failed on cache " + address(a0));
            }
         } catch (AvailabilityException e) {
         }
      }

      log.debug("Changing partition availability mode back to AVAILABLE");
      cchf.setOwnerIndexes(new int[]{a0, a1}, new int[]{a1, a0},
            new int[]{a0, a1}, new int[]{a1, a0});
      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(a0), LocalTopologyManager.class);
      ltm.setCacheAvailability(CacheContainer.DEFAULT_CACHE_NAME, AvailabilityMode.AVAILABLE);
      TestingUtil.waitForRehashToComplete(cache(a0), cache(a1));
      eventuallyEquals(AvailabilityMode.AVAILABLE, phm0::getAvailabilityMode);
   }

   private void installNewView(List<Address> members, Address missing, EmbeddedCacheManager... where) {
      log.tracef("installNewView:members=%s, missing=%s", members, missing);
      final List<org.jgroups.Address> viewMembers = new ArrayList<org.jgroups.Address>();
      for (Address a : members)
         if (!a.equals(missing))
            viewMembers.add(((JGroupsAddress) a).getJGroupsAddress());
      int viewId = where[0].getTransport().getViewId() + 1;
      View view = View.create(viewMembers.get(0), viewId, viewMembers.toArray(new org.jgroups.Address[viewMembers.size()]));

      log.trace("Before installing new view:" + viewMembers);
      for (EmbeddedCacheManager ecm : where) {
         JChannel c = ((JGroupsTransport) ecm.getTransport()).getChannel();
         ((GMS) c.getProtocolStack().findProtocol(GMS.class)).installView(view);
      }
   }

   /**
    * Simulates a node crash, discarding all the messages from/to this node and then stopping the caches.
    */
   protected void crashCacheManagers(EmbeddedCacheManager... cacheManagers) {
      for (EmbeddedCacheManager cm : cacheManagers) {
         JGroupsTransport t = (JGroupsTransport) cm.getGlobalComponentRegistry().getComponent(Transport.class);
         JChannel channel = t.getChannel();
         try {
            DISCARD discard = new DISCARD();
            discard.setDiscardAll(true);
            channel.getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);
         } catch (Exception e) {
            log.warn("Problems inserting discard", e);
            throw new RuntimeException(e);
         }
         View view = View.create(channel.getAddress(), 100, channel.getAddress());
         ((GMS) channel.getProtocolStack().findProtocol(GMS.class)).installView(view);
      }
      TestingUtil.killCacheManagers(cacheManagers);
   }

}
