package org.infinispan.partitionhandling;

import static org.infinispan.test.concurrent.StateSequencerUtil.advanceOnInboundRpc;
import static org.infinispan.test.concurrent.StateSequencerUtil.matchCommand;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.Collection;
import java.util.List;

import org.infinispan.commands.statetransfer.StateResponseCommand;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.distribution.MagicKey;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.impl.PartitionHandlingManager;
import org.infinispan.remoting.transport.Address;
import org.infinispan.test.MultipleCacheManagersTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.concurrent.StateSequencer;
import org.infinispan.topology.LocalTopologyManager;
import org.infinispan.util.ControlledConsistentHashFactory;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;
import org.testng.annotations.Test;

/**
 * With a cluster made out of nodes {A,B,C,D}, tests that D crashes and before the state transfer finishes, another node
 * C crashes. {A,B} should enter in degraded mode. The only way in which it could recover is explicitly, through JMX
 * operations.
 */
@Test(groups = "functional", testName = "partitionhandling.NumOwnersNodeCrashInSequenceTest")
public class NumOwnersNodeCrashInSequenceTest extends MultipleCacheManagersTest {

   private static final Log log = LogFactory.getLog(NumOwnersNodeCrashInSequenceTest.class);

   ControlledConsistentHashFactory cchf;
   private ConfigurationBuilder configBuilder;
   protected AvailabilityMode expectedAvailabilityMode;

   public NumOwnersNodeCrashInSequenceTest() {
      cleanup = CleanupPhase.AFTER_METHOD;
      expectedAvailabilityMode = AvailabilityMode.DEGRADED_MODE;
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      cchf = new ControlledConsistentHashFactory.Default(new int[][]{{0, 1}, {1, 2}, {2, 3}, {3, 0}});
      configBuilder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC);
      configBuilder.clustering().partitionHandling().whenSplit(PartitionHandling.DENY_READ_WRITES);
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

      cchf.setOwnerIndexes(new int[][]{{a0, a1}, {a1, c0}, {c0, c1}, {c1, a0}});
      configBuilder.clustering().hash().consistentHashFactory(cchf);
      createCluster(ControlledConsistentHashFactory.SCI.INSTANCE, configBuilder, 4);
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
      cchf.setOwnerIndexes(new int[][]{{a0, a1}, {a1, c0}, {c0, a1}, {c0, a0}});

      Address address1 = address(c1);
      log.tracef("Before killing node %s", address1);
      crashCacheManagers(manager(c1));
      installNewView(advancedCache(a0).getRpcManager().getTransport().getMembers(), address1, manager(a0), manager(a1)
            , manager(c0));

      ss.enter("main:2nd_node_left");

      Address address0 = address(c0);
      log.tracef("Killing 2nd node %s", address0);
      crashCacheManagers(manager(c0));
      installNewView(advancedCache(a0).getRpcManager().getTransport().getMembers(), address0, manager(a0), manager(a1));

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
      LocalizedCacheTopology topology = cache(a0).getAdvancedCache().getDistributionManager().getCacheTopology();
      assertEquals(3, topology.getMembers().size());
      for (Object k : allKeys) {
         Collection<Address> owners = topology.getDistribution(k).readOwners();
         try {
            cache(a0).get(k);
            if (owners.contains(address0) || owners.contains(address1)) {
               fail("get(" + k + ") should have failed on cache " + address(a0));
            }
         } catch (AvailabilityException e) {
         }
         try {
            cache(a1).put(k, k);
            if (owners.contains(address0) || owners.contains(address1)) {
               fail("put(" + k + ", v) should have failed on cache " + address(a0));
            }
         } catch (AvailabilityException e) {
         }
      }

      log.debug("Changing partition availability mode back to AVAILABLE");
      cchf.setOwnerIndexes(new int[][]{{a0, a1}, {a1, a0}, {a0, a1}, {a1, a0}});
      LocalTopologyManager ltm = TestingUtil.extractGlobalComponent(manager(a0), LocalTopologyManager.class);
      ltm.setCacheAvailability(TestingUtil.getDefaultCacheName(manager(a0)), AvailabilityMode.AVAILABLE);
      TestingUtil.waitForNoRebalance(cache(a0), cache(a1));
      eventuallyEquals(AvailabilityMode.AVAILABLE, phm0::getAvailabilityMode);
   }

   private void installNewView(List<Address> members, Address missing, EmbeddedCacheManager... where) {
      TestingUtil.installNewView(members.stream().filter(a -> !a.equals(missing)), where);
   }

   protected void crashCacheManagers(EmbeddedCacheManager... cacheManagers) {
      TestingUtil.crashCacheManagers(cacheManagers);
   }
}
