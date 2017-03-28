package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_BASIC;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_TOPOLOGY_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertKeyDoesNotExist;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
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
import org.testng.annotations.Test;

/**
 * Tests Hot Rod with a local (non-clustered) cache using Hot Rod's 1.1 protocol.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRod11LocalCacheTest")
public class HotRod11LocalCacheTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "localVersion11";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.LOCAL, false));
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
      assertEquals(resp.topologyResponse, null);

      // Client intelligence is now 1, which means no topology updates
      resp = client1.put(k(m), 0, 0, v(m), INTELLIGENCE_BASIC, 0);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
      assertKeyDoesNotExist(client2.get(k(m), 0));

      resp = client2.put(k(m), 0, 0, v(m, "v1-"), INTELLIGENCE_TOPOLOGY_AWARE, 0);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);
      // check that client1 still has the old value
      assertSuccess(client1.get(k(m), 0), v(m));

      HotRodServer newServer = startClusteredServer(servers().get(1).getPort() + 25);
      HotRodClient newClient = new HotRodClient("127.0.0.1", newServer.getPort(), cacheName(), 60, protocolVersion());
      List<HotRodServer> allServers =
            Stream.concat(Stream.of(newServer), servers().stream()).collect(Collectors.toList());
      try {
         log.trace("New client started, modify key to be v6-*");
         resp = newClient.put(k(m), 0, 0, v(m, "v2-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         assertEquals(resp.topologyResponse, null);

         log.trace("Get key from the other clients and verify that it hasn't changed");
         assertSuccess(client1.get(k(m), 0), v(m));
         assertSuccess(client2.get(k(m), 0), v(m, "v1-"));

         resp = client2.put(k(m), 0, 0, v(m, "v3-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
         assertStatus(resp, Success);
         assertEquals(resp.topologyResponse, null);

         // now the new client's value doesn't change
         assertSuccess(newClient.get(k(m), 0), v(m, "v2-"));
      } finally {
         log.trace("Stopping new server");
         killClient(newClient);
         stopClusteredServer(newServer);
         log.trace("New server stopped");
      }

      resp = client2.put(k(m), 0, 0, v(m, "v4-"), (byte) 3, 2);
      assertStatus(resp, Success);
      assertEquals(resp.topologyResponse, null);

      assertSuccess(client1.get(k(m), 0), v(m));
      assertSuccess(client2.get(k(m), 0), v(m, "v4-"));
   }


}
