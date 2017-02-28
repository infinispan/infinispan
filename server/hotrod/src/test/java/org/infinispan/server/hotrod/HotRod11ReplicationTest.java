package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_BASIC;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_TOPOLOGY_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertTopologyReceived;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.killClient;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.ClusterCacheStatus;
import org.testng.annotations.Test;

/**
 * Tests Hot Rod replication mode using Hot Rod's 1.1 protocol.
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRod11ReplicationTest")
public class HotRod11ReplicationTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "replicateVersion11";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
   }

   @Override
   protected byte protocolVersion() {
      return 11;
   }

   protected int virtualNodes() {
      return 1;
   }

   public void testDistributedPutWithTopologyChanges(Method m) {
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);

      TestResponse resp = client1.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      // Client intelligence is now 1, which means no topology updates
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
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"));

      resp = client2.put(k(m), 0, 0, v(m, "v5-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());
      assertSuccess(client2.get(k(m), 0), v(m, "v5-"));

      HotRodServer newServer = startClusteredServer(servers().get(1).getPort() + 25);
      HotRodClient newClient = new HotRodClient(
            "127.0.0.1", newServer.getPort(), cacheName(), 60, protocolVersion());
      List<HotRodServer> allServers =
            Stream.concat(Stream.of(newServer), servers().stream()).collect(Collectors.toList());
      try {
         log.trace("New client started, modify key to be v6-*");
         resp = newClient.put(k(m), 0, 0, v(m, "v6-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         assertTopologyReceived(resp.topologyResponse, allServers, currentServerTopologyId());

         log.trace("Get key from other client and verify that's v6-*");
         assertSuccess(client2.get(k(m), 0), v(m, "v6-"));

         resp = client2.put(k(m), 0, 0, v(m, "v7-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         assertTopologyReceived(resp.topologyResponse, allServers, currentServerTopologyId());

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
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      assertSuccess(client1.get(k(m), 0), v(m, "v8-"));
   }


}
