package org.infinispan.server.hotrod;

import static org.infinispan.commons.api.BasicCacheContainer.DEFAULT_CACHE_NAME;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHashTopology20Received;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.partitionhandling.BasePartitionHandlingTest;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TransportFlags;
import org.infinispan.topology.CacheTopology;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "server.hotrod.HotRodMergeTest")
public class HotRodMergeTest extends BasePartitionHandlingTest {

   private List<HotRodServer> servers = new ArrayList<>();
   private HotRodClient client;

   public HotRodMergeTest() {
      numMembersInCluster = 2;
      cacheMode = CacheMode.DIST_SYNC;
      cleanup = CleanupPhase.AFTER_TEST;
   }

   @BeforeClass(alwaysRun = true)
   @Override
   public void createBeforeClass() throws Throwable {
      super.createBeforeClass();

      int nextServerPort = serverPort();
      for (int i = 0; i < numMembersInCluster; i++) {
         servers.add(startHotRodServer(cacheManagers.get(i), nextServerPort));
         nextServerPort += 50;
      }

      client = new HotRodClient("127.0.0.1", servers.get(0).getPort(), DEFAULT_CACHE_NAME, 60, (byte) 21);
      TestingUtil.waitForNoRebalance(cache(0), cache(1));
   }

   @AfterClass(alwaysRun = true)
   @Override
   protected void destroy() {
      try {
         killClient(client);
         servers.forEach(ServerTestingUtil::killServer);
      } finally {
         super.destroy();
      }
   }

   @Override
   protected void createCacheManagers() {
      ConfigurationBuilder dcc = hotRodCacheConfiguration(new ConfigurationBuilder());
      dcc.clustering().cacheMode(cacheMode).hash().numOwners(1);
      createClusteredCaches(numMembersInCluster, dcc, new TransportFlags().withFD(true).withMerge(true));
      waitForClusterToForm();
   }

   public void testNewTopologySentAfterCleanMerge() {
      TestingUtil.waitForNoRebalanceAcrossManagers(managers());
      int initialTopology = advancedCache(0).getRpcManager().getTopologyId();

      expectCompleteTopology(client, initialTopology);
      PartitionDescriptor p0 = new PartitionDescriptor(0);
      PartitionDescriptor p1 = new PartitionDescriptor(1);
      splitCluster(p0.getNodes(), p1.getNodes());
      TestingUtil.waitForNoRebalance(cache(p1.node(0)));
      TestingUtil.waitForNoRebalance(cache(p0.node(0)));
      expectPartialTopology(client, initialTopology + 1);
      partition(0).merge(partition(1));
      eventuallyExpectCompleteTopology(client, initialTopology + 8);
      // Check that we got the number of topology updates to NO_REBALANCE right
      // T+2: NO_REBALANCE in partition [B] before merge
      // T+3: CONFLICT_RESOLUTION, preferred CH: owners = (1) [test-NodeA-22368: 256+0]
      // T+4: NO_REBALANCE update topology after CR and before rebalance begins
      // T+5:READ_OLD (rebalance starts), T+6:READ_ALL, T+7:READ_NEW, T+8: NO_REBALANCE
      LocalizedCacheTopology newTopology = advancedCache(0).getDistributionManager().getCacheTopology();
      assertEquals(CacheTopology.Phase.NO_REBALANCE, newTopology.getPhase());
      assertEquals(initialTopology + 8, newTopology.getTopologyId());
   }

   public void testNewTopologySentAfterOverlappingMerge() {
      TestingUtil.waitForNoRebalanceAcrossManagers(managers());
      int initialTopology = advancedCache(0).getRpcManager().getTopologyId();
      expectCompleteTopology(client, initialTopology);
      PartitionDescriptor p1 = new PartitionDescriptor(0);
      // isolatePartitions will always result in a CR fail as Node 0 tries to contact Node 1 in order to receive segments
      // which is not possible as all messages received by Node 1 from Node 0 are discarded by the DISCARD protocol.
      // Therefore, it is necessary for the state transfer timeout to be < then the timeout utilised by TestingUtil::waitForNoRebalance
      isolatePartition(p1.getNodes());
      TestingUtil.waitForNoRebalance(cache(p1.node(0)));
      eventuallyExpectPartialTopology(client, initialTopology + 1);

      partition(0).merge(partition(1));
      eventuallyExpectCompleteTopology(client, initialTopology + 7);
      // Check that we got the number of topology updates to NO_REBALANCE right
      // T+2: CONFLICT_RESOLUTION, preferred CH: owners = (1) [test-NodeA-22368: 256+0]
      // T+3: NO_REBALANCE update topology after CR and before rebalance begins
      // T+4:READ_OLD (rebalance starts), T+5:READ_ALL, T+6:READ_NEW, T+7: NO_REBALANCE
      LocalizedCacheTopology newTopology = advancedCache(0).getDistributionManager().getCacheTopology();
      assertEquals(CacheTopology.Phase.NO_REBALANCE, newTopology.getPhase());
   }


   private void eventuallyExpectCompleteTopology(HotRodClient c, int expectedTopologyId) {
      eventually(() -> {
         TestResponse resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         if (resp.topologyResponse == null || (resp.topologyResponse.topologyId < expectedTopologyId)) {
            return false;
         }
         assertHashTopology20Received(resp.topologyResponse, servers, DEFAULT_CACHE_NAME, expectedTopologyId);
         return true;
      });
   }

   private void expectCompleteTopology(HotRodClient c, int expectedTopologyId) {
      TestResponse resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertHashTopology20Received(resp.topologyResponse, servers, DEFAULT_CACHE_NAME, expectedTopologyId);
   }

   private void eventuallyExpectPartialTopology(HotRodClient c, int expectedTopologyId) {
      eventually(() -> {
         TestResponse resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         if (resp.topologyResponse == null || (resp.topologyResponse.topologyId < expectedTopologyId)) {
            return false;
         }
         assertHashTopology20Received(resp.topologyResponse, Arrays.asList(servers.get(0)), DEFAULT_CACHE_NAME,
                                      expectedTopologyId);
         return true;
      });
   }

   private void expectPartialTopology(HotRodClient c, int expectedTopologyId) {
      TestResponse resp = c.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertHashTopology20Received(resp.topologyResponse, Arrays.asList(servers.get(0)), DEFAULT_CACHE_NAME,
                                   expectedTopologyId);
   }

}
