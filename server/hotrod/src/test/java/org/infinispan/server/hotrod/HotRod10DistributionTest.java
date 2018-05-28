package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_BASIC;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_TOPOLOGY_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHashTopology10Received;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertTopologyReceived;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.ClusterCacheStatus;
import org.testng.annotations.Test;


/**
 * Tests Hot Rod logic when interacting with distributed caches, particularly logic to do with
 * hash-distribution-aware headers and how it behaves when cluster formation changes.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRod10DistributionTest")
public class HotRod10DistributionTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "hotRodDistSync";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder cfg = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfg.clustering().l1().disable(); // Disable L1 explicitly
      return cfg;
   }

   @Override
   protected byte protocolVersion() {
      return 10;
   }

   public void testDistributedPutWithTopologyChanges(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);

      TestResponse resp = client1.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertHashTopology10Received(resp.topologyResponse, servers(), cacheName(), currentServerTopologyId());

      resp = client1.put(k(m), 0, 0, v(m), INTELLIGENCE_BASIC, 0);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
      assertSuccess(client2.get(k(m), 0), v(m));

      resp = client1.put(k(m), 0, 0, v(m, "v1-"), INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      resp = client2.put(k(m), 0, 0, v(m, "v2-"), INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      resp = client1.put(k(m), 0, 0, v(m, "v3-"), INTELLIGENCE_TOPOLOGY_AWARE,
                         ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount());
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
      assertSuccess(client2.get(k(m), 0), v(m, "v3-"));

      resp = client1.put(k(m), 0, 0, v(m, "v4-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertHashTopology10Received(resp.topologyResponse, servers(), cacheName(), currentServerTopologyId());
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"));

      resp = client2.put(k(m), 0, 0, v(m, "v5-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertHashTopology10Received(resp.topologyResponse, servers(), cacheName(), currentServerTopologyId());
      assertSuccess(client2.get(k(m), 0), v(m, "v5-"));

      HotRodServer newServer = startClusteredServer(servers().get(1).getPort() + 25);
      HotRodClient newClient = new HotRodClient(
            "127.0.0.1", newServer.getPort(), cacheName(), 60, protocolVersion());
      List<HotRodServer> allServers = new ArrayList<>(servers());
      allServers.add(0, newServer);
      try {
         log.trace("New client started, modify key to be v6-*");
         resp = newClient.put(k(m), 0, 0, v(m, "v6-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         assertHashTopology10Received(resp.topologyResponse, allServers, cacheName(), currentServerTopologyId());

         log.trace("Get key and verify that's v6-*");
         assertSuccess(client2.get(k(m), 0), v(m, "v6-"));

         resp = client2.put(k(m), 0, 0, v(m, "v7-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         assertHashTopology10Received(resp.topologyResponse, allServers, cacheName(), currentServerTopologyId());

         assertSuccess(newClient.get(k(m), 0), v(m, "v7-"));
      } finally {
         log.trace("Stopping new server");
         killClient(newClient);
         stopClusteredServer(newServer);
         TestingUtil.waitForNoRebalance(cache(0, cacheName()), cache(1, cacheName()));
         log.trace("New server stopped");
      }

      resp = client2.put(k(m), 0, 0, v(m, "v8-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE,
                         ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount());
      assertStatus(resp, Success);
      assertHashTopology10Received(resp.topologyResponse, servers(), cacheName(), currentServerTopologyId());

      assertSuccess(client1.get(k(m), 0), v(m, "v8-"));
   }
}
