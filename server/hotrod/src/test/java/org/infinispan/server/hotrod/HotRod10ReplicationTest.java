package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_BASIC;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_TOPOLOGY_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.OperationNotExecuted;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertKeyDoesNotExist;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertNoHashTopologyReceived;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertTopologyReceived;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Stream;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.infinispan.topology.ClusterCacheStatus;
import org.testng.annotations.Test;

/**
 * Tests Hot Rod instances configured with replication in protocol version 1.0.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRod10ReplicationTest")
public class HotRod10ReplicationTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "hotRodReplSync";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder config = hotRodCacheConfiguration(
            getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      config.clustering().stateTransfer().fetchInMemoryState(true);
      return config;
   }

   @Override
   protected byte protocolVersion() {
      return 10;
   }

   public void testReplicatedPut(Method m) {
      TestResponse resp = clients().get(0).put(k(m), 0, 0, v(m));
      assertStatus(resp, Success);
      assertSuccess(clients().get(1).get(k(m), 0), v(m));
   }

   public void testReplicatedPutIfAbsent(Method m) {
      assertKeyDoesNotExist(clients().get(0).assertGet(m));
      assertKeyDoesNotExist(clients().get(1).assertGet(m));
      TestResponse resp = clients().get(0).putIfAbsent(k(m), 0, 0, v(m));
      assertStatus(resp, Success);
      assertSuccess(clients().get(1).get(k(m), 0), v(m));
      assertStatus(clients().get(1).putIfAbsent(k(m), 0, 0, v(m, "v2-")), OperationNotExecuted);
   }

   public void testReplicatedReplace(Method m) {
      TestResponse resp = clients().get(0).replace(k(m), 0, 0, v(m));
      assertStatus(resp, OperationNotExecuted);
      resp = clients().get(1).replace(k(m), 0, 0, v(m));
      assertStatus(resp, OperationNotExecuted);
      clients().get(1).assertPut(m);
      resp = clients().get(1).replace(k(m), 0, 0, v(m, "v1-"));
      assertStatus(resp, Success);
      assertSuccess(clients().get(0).assertGet(m), v(m, "v1-"));
      resp = clients().get(0).replace(k(m), 0, 0, v(m, "v2-"));
      assertStatus(resp, Success);
      assertSuccess(clients().get(1).assertGet(m), v(m, "v2-"));
   }

   public void testPingWithTopologyAwareClient() {
      TestResponse resp = clients().get(0).ping();
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);

      resp = clients().get(1).ping(INTELLIGENCE_BASIC, 0);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);

      resp = clients().get(0).ping(INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      resp = clients().get(1).ping(INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      resp = clients().get(1).ping(INTELLIGENCE_TOPOLOGY_AWARE, ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount());
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
   }

   public void testReplicatedPutWithTopologyChanges(Method m) {
      TestResponse resp = clients().get(0).put(k(m), 0, 0, v(m), INTELLIGENCE_BASIC, 0);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
      assertSuccess(clients().get(1).get(k(m), 0), v(m));

      resp = clients().get(0).put(k(m), 0, 0, v(m, "v1-"), INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      resp = clients().get(1).put(k(m), 0, 0, v(m, "v2-"), INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertTopologyReceived(resp.topologyResponse, servers(), currentServerTopologyId());

      resp = clients().get(0).put(k(m), 0, 0, v(m, "v3-"), INTELLIGENCE_TOPOLOGY_AWARE,
                                  ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount());
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
      assertSuccess(clients().get(1).get(k(m), 0), v(m, "v3-"));

      HotRodServer newServer = startClusteredServer(servers().get(1).getPort() + 25);
      try {
         resp = clients().get(0).put(k(m), 0, 0, v(m, "v4-"), INTELLIGENCE_TOPOLOGY_AWARE,
                                     ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount());
         assertStatus(resp, Success);
         assertEquals(resp.topologyResponse.topologyId, currentServerTopologyId());
         AbstractTestTopologyAwareResponse topoResp = resp.asTopologyAwareResponse();
         assertEquals(topoResp.members.size(), nodeCount() + 1);
         Stream.concat(Stream.of(newServer), servers().stream())
               .map(HotRodServer::getAddress)
               .forEach(addr -> assertTrue(topoResp.members.contains(addr)));
         assertSuccess(clients().get(1).get(k(m), 0), v(m, "v4-"));
      } finally {
         stopClusteredServer(newServer);
         TestingUtil.waitForNoRebalance(cache(0, cacheName()), cache(1, cacheName()));
      }

      resp = clients().get(0).put(k(m), 0, 0, v(m, "v5-"), INTELLIGENCE_TOPOLOGY_AWARE,
                                  ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount() + 2);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse.topologyId, currentServerTopologyId());
      AbstractTestTopologyAwareResponse topoResp1 = resp.asTopologyAwareResponse();
      assertEquals(topoResp1.members.size(), nodeCount());
      servers().stream()
               .map(HotRodServer::getAddress)
               .forEach(addr -> assertTrue(topoResp1.members.contains(addr)));
      assertSuccess(clients().get(1).get(k(m), 0), v(m, "v5-"));

      HotRodServer crashingServer = startClusteredServer(servers().get(1).getPort() + 25);
      try {
         resp = clients().get(0).put(k(m), 0, 0, v(m, "v6-"), INTELLIGENCE_TOPOLOGY_AWARE,
                                     ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount() + 4);
         assertStatus(resp, Success);
         assertEquals(resp.topologyResponse.topologyId, currentServerTopologyId());
         AbstractTestTopologyAwareResponse topoResp2 = resp.asTopologyAwareResponse();
         assertEquals(topoResp2.members.size(), nodeCount() + 1);
         Stream.concat(Stream.of(crashingServer), servers().stream())
               .map(HotRodServer::getAddress)
               .forEach(addr -> assertTrue(topoResp2.members.contains(addr)));
         assertSuccess(clients().get(1).get(k(m), 0), v(m, "v6-"));
      } finally {
         stopClusteredServer(crashingServer);
         TestingUtil.waitForNoRebalance(cache(0, cacheName()), cache(1, cacheName()));
      }

      resp = clients().get(0).put(k(m), 0, 0, v(m, "v7-"), INTELLIGENCE_TOPOLOGY_AWARE,
                                  ClusterCacheStatus.INITIAL_TOPOLOGY_ID + 2 * nodeCount() + 6);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse.topologyId, currentServerTopologyId());
      AbstractTestTopologyAwareResponse topoResp3 = resp.asTopologyAwareResponse();
      assertEquals(topoResp3.members.size(), nodeCount());
      servers().stream()
               .map(HotRodServer::getAddress)
               .forEach(addr -> assertTrue(topoResp3.members.contains(addr)));
      assertSuccess(clients().get(1).get(k(m), 0), v(m, "v7-"));

      resp = clients().get(0).put(k(m), 0, 0, v(m, "v8-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE,
                                  ClusterCacheStatus.INITIAL_TOPOLOGY_ID + nodeCount());
      assertStatus(resp, Success);

      checkTopologyReceived(resp.topologyResponse, servers(), cacheName());
      assertSuccess(clients().get(1).get(k(m), 0), v(m, "v8-"));
   }

   protected void checkTopologyReceived(AbstractTestTopologyAwareResponse topoResp,
                                        List<HotRodServer> servers, String cacheName) {
      assertNoHashTopologyReceived(topoResp, servers, cacheName, currentServerTopologyId());
   }

}
