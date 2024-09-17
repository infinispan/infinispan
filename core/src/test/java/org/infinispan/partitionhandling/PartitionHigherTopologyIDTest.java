package org.infinispan.partitionhandling;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.fail;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import org.infinispan.Cache;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.factories.ComponentRegistry;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.ClusterCacheStatus;
import org.infinispan.topology.ClusterTopologyManager;
import org.jgroups.JChannel;
import org.jgroups.protocols.MERGE3;
import org.testng.annotations.Test;

/**
 * Partition tests when one sub group has a higher topology than other sub groups.
 */
@Test(groups = "functional", testName = "conflict.impl.MultipleCachesDuringConflictResolutionTest")
public class PartitionHigherTopologyIDTest extends BasePartitionHandlingTest {

    private final PartitionDescriptor p0;
    private final PartitionDescriptor p1;

    public PartitionHigherTopologyIDTest() {
        this.p0 = new PartitionDescriptor(0, 1);
        this.p1 = new PartitionDescriptor(2, 3);
        this.partitionHandling = PartitionHandling.ALLOW_READ_WRITES;
    }

    public void testPartitionWithDifferentTopologyIds() {
        TestingUtil.waitForNoRebalance(caches());

        log.tracef("split test");
        splitCluster(p0.getNodes(), p1.getNodes());
        TestingUtil.waitForNoRebalance(caches());

        Cache<?, ?> c = cache(0);

        for (int i = 0; i < 10; ++i) {
            ClusterTopologyManager ctm = ComponentRegistry.componentOf(c, ClusterTopologyManager.class);
            ConcurrentMap<String, ClusterCacheStatus> map = TestingUtil.extractField(ctm, "cacheStatusMap");
            ClusterCacheStatus ccs = map.get(c.getName());

            ccs.manuallyUpdateAvailabilityMode(List.of(address(0), address(1)), AvailabilityMode.DEGRADED_MODE, false);
            ccs.manuallyUpdateAvailabilityMode(List.of(address(0), address(1)), AvailabilityMode.AVAILABLE, false);
        }

        // Wait until the first and second nodes have the same topologyId
        eventually(() ->
            cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId() ==
                  cache(1).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId()
        );

        int firstTopology = cache(0).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();

        int thirdTopology = cache(2).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
        int fourthTopology = cache(3).getAdvancedCache().getDistributionManager().getCacheTopology().getTopologyId();
        assertEquals(thirdTopology, fourthTopology);

        if (thirdTopology >= firstTopology + 10) {
            fail("The third/fourth topology " + thirdTopology + " should be less than the first/second " + firstTopology);
        }

        log.tracef("performMerge");
        partition(0).merge(partition(1));
        TestingUtil.waitForNoRebalance(caches());

        for (Cache<?, ?> cache : caches()) {
            LocalizedCacheTopology topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
            assertEquals(4, topology.getMembers().size());
        }
    }

    public void testAsymmetricPartition() throws InterruptedException {
        // Disable discovery
        for (int i = 0; i < numMembersInCluster; i++) {
            JChannel channel = channel(cache(i));
            disableDiscoveryProtocol(channel);

            // Effectively disable MERGE3 protocol
            MERGE3 merge3 = channel.getProtocolStack().findProtocol(MERGE3.class);
            merge3.setMaxInterval(60000);
            merge3.setMinInterval(60000);
        }

        // Note we only split the first partition. The second "partition" will still have all nodes but its topology
        // is not increased until it finally merges back together.
        // Manager 0 view -> {0, 1}
        // Manager 1 view -> {0, 1}
        // Manager 2 view -> {0, 1, 2, 3}
        // Manager 3 view -> {0, 1, 2, 3}
        asymmetricSplitCluster(p0.getNodes());

        TestingUtil.waitForNoRebalance(getPartitionCaches(p0));

        log.fatal("Rebalances complete, merging back together!");

        partition(0).merge(partition(1));

        TestingUtil.waitForNoRebalance(caches());

        for (Cache<?, ?> cache : caches()) {
            LocalizedCacheTopology topology = cache.getAdvancedCache().getDistributionManager().getCacheTopology();
            assertEquals(4, topology.getMembers().size());
        }
    }
}
