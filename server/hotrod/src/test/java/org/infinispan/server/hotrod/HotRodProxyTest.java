package org.infinispan.server.hotrod;

import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.serverPort;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.Set;
import java.util.stream.Collectors;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.test.AbstractTestTopologyAwareResponse;
import org.infinispan.server.hotrod.test.TestResponse;
import org.testng.annotations.Test;

/**
 * Tests Hot Rod instances that are behind a proxy.
 *
 * @author Galder Zamarreño
 * @since 4.1
 */
@Test(groups = "functional", testName = "server.hotrod.HotRodProxyTest")
public class HotRodProxyTest extends HotRodMultiNodeTest {

   private String proxyHost1 = "1.2.3.4";
   private String proxyHost2 = "2.3.4.5";
   private int proxyPort1 = 8123;
   private int proxyPort2 = 9123;

   @Override
   protected String cacheName() {
      return "hotRodProxy";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      ConfigurationBuilder config =
            hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false));
      config.clustering().stateTransfer().fetchInMemoryState(true);
      return config;
   }

   @Override
   protected HotRodServer startTestHotRodServer(EmbeddedCacheManager cacheManager, int port) {
      if (port == serverPort())
         return startHotRodServer(cacheManager, proxyHost1, proxyPort1);
      else
         return startHotRodServer(cacheManager, port, proxyHost2, proxyPort2);
   }

   public void testTopologyWithProxiesReturned() {
      TestResponse resp = clients().get(0).ping((byte) 2, 0);
      assertStatus(resp, Success);
      AbstractTestTopologyAwareResponse topoResp = resp.asTopologyAwareResponse();
      assertEquals(topoResp.topologyId, currentServerTopologyId());
      assertEquals(topoResp.members.size(), 2);
      Set<ServerAddress> serverAddresses = servers().stream()
                                                    .map(HotRodServer::getAddress)
                                                    .collect(Collectors.toSet());
      topoResp.members.forEach(member -> assertTrue(serverAddresses.contains(member)));
   }

}
