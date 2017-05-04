package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.Constants.INTELLIGENCE_HASH_DISTRIBUTION_AWARE;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertHashTopologyReceived;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertSuccess;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.k;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.v;
import static org.testng.AssertJUnit.assertNull;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Optional;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.core.test.ServerTestingUtil;
import org.infinispan.server.hotrod.test.HotRodClient;
import org.infinispan.server.hotrod.test.HotRodMagicKeyGenerator;
import org.infinispan.server.hotrod.test.TestResponse;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.Test;

/**
 * Tests Hot Rod distribution mode when some of the cache managers do not have HotRod servers running.
 *
 * @author Dan Berindei
 * @since 5.2
 */
@Test(groups = "functional", testName = "server.hotrod.HotRod11StorageOnlyNodesTest")
public class HotRod11StorageOnlyNodesTest extends HotRodMultiNodeTest {

   @Override
   protected String cacheName() {
      return "distributed";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder cfg = hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
      cfg.clustering().l1().disable(); // Disable L1 explicitly
      return cfg;
   }

   @Override
   protected byte protocolVersion() { return 11; }

   protected int virtualNodes() { return 1; }

   public void testAddingStorageOnlyNode(Method m) throws Exception {
      HotRodServer server1 = servers().get(0);
      HotRodServer server2 = servers().get(1);
      HotRodClient client1 = clients().get(0);
      HotRodClient client2 = clients().get(1);
      int initialTopologyId = currentServerTopologyId();

      TestResponse resp = client1.ping(INTELLIGENCE_HASH_DISTRIBUTION_AWARE, 0);
      assertStatus(resp, Success);
      assertHashTopologyReceived(resp.topologyResponse, servers(), cacheName(), 2, virtualNodes(), initialTopologyId);

      EmbeddedCacheManager newCacheManager = addClusterEnabledCacheManager();
      Optional<Integer> leaveTopologyId;
      try {
         newCacheManager.defineConfiguration(cacheName(), createCacheConfig().build());
         newCacheManager.getCache(cacheName());
         TestingUtil.blockUntilViewsReceived(50000, true, manager(0), manager(1), manager(2));
         TestingUtil.waitForNoRebalance(cache(0, cacheName()), cache(1, cacheName()), cache(2, cacheName()));
         int joinTopologyId = currentServerTopologyId();

         // The clients receive a new topology (because the rebalance increased the topology id by 2)
         // but the storage-only node is not included in the new topology.
         byte[] key1 = HotRodMagicKeyGenerator.newKey(cache(0, cacheName()));
         resp = client1.put(key1, 0, 0, v(m, "v1-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, initialTopologyId);
         assertStatus(resp, Success);
         assertHashTopologyReceived(resp.topologyResponse, servers(), cacheName(), 2, virtualNodes(), joinTopologyId - 1);

         // The clients won't receive another topology (the client topology will stay behind the
         // server topology id by 1).
         log.trace("Check that the clients do not receive a new topology");
         resp = client1.put(key1, 0, 0, v(m, "v1-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, joinTopologyId - 1);
         assertStatus(resp, Success);
         assertNull(resp.topologyResponse);

         log.trace("Check that the clients can access a key for which the storage-only node is primary owner");
         byte[] key2 = HotRodMagicKeyGenerator.newKey(cache(2, cacheName()));
         resp = client1.put(key2, 0, 0, v(m, "v2-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, joinTopologyId - 1);
         assertStatus(resp, Success);
         assertNull(resp.topologyResponse);

         assertSuccess(client2.get(key1, 0), v(m, "v1-"));
         assertSuccess(client2.get(key2, 0), v(m, "v2-"));

         log.trace("Force a topology change by shutting down one of the proper HotRod servers");
         ServerTestingUtil.killServer(server2);
         TestingUtil.killCacheManagers(servers().get(1).getCacheManager());
         TestingUtil.blockUntilViewsReceived(50000, false, manager(0), manager(2));
         TestingUtil.waitForNoRebalance(cache(0, cacheName()), cache(2, cacheName()));
         leaveTopologyId = Optional.of(currentServerTopologyId());

         resp = client1.put(key1, 0, 0, v(m, "v3-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, joinTopologyId - 1);
         assertStatus(resp, Success);
         assertHashTopologyReceived(resp.topologyResponse, Arrays.asList(server1), cacheName(), 2, virtualNodes(),
                                    leaveTopologyId.get() - 1);
      } finally {
         TestingUtil.killCacheManagers(newCacheManager);
         TestingUtil.blockUntilViewsReceived(50000, false, manager(0));
         TestingUtil.waitForNoRebalance(cache(0, cacheName()));
      }

      int storageOnlyLeaveTopologyId = currentServerTopologyId();
      log.trace("Check that only the topology id changes after the storage-only server is killed");
      resp = client1.put(k(m), 0, 0, v(m, "v4-"), INTELLIGENCE_HASH_DISTRIBUTION_AWARE, leaveTopologyId.get() - 1);
      assertStatus(resp, Success);
      assertHashTopologyReceived(resp.topologyResponse, Arrays.asList(server1), cacheName(), 2, 1, storageOnlyLeaveTopologyId);

      assertSuccess(client1.get(k(m), 0), v(m, "v4-"));
   }


}
