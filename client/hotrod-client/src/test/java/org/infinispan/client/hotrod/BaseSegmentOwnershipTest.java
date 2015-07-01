package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.MultiHotRodServersTest;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.test.HotRodTestingUtil;

import java.net.SocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author gustavonalle
 * @since 8.0
 */
public abstract class BaseSegmentOwnershipTest extends MultiHotRodServersTest {

   static final int NUM_SEGMENTS = 20;
   static final int NUM_OWNERS = 2;
   static final int NUM_SERVERS = 3;

   protected Map<Integer, Set<SocketAddress>> invertMap(Map<SocketAddress, Set<Integer>> segmentsByServer) {
      Map<Integer, Set<SocketAddress>> serversBySegment = new HashMap<>();
      for (Map.Entry<SocketAddress, Set<Integer>> entry : segmentsByServer.entrySet()) {
         for (Integer seg : entry.getValue()) {
            serversBySegment.computeIfAbsent(seg, v -> new HashSet<>()).add(entry.getKey());
         }
      }
      return serversBySegment;
   }

   protected abstract CacheMode getCacheMode();

   protected ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(getCacheMode(), false);
      builder.clustering().hash().numOwners(NUM_OWNERS).numSegments(NUM_SEGMENTS);
      return HotRodTestingUtil.hotRodCacheConfiguration(builder);
   }

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   @Override
   protected org.infinispan.client.hotrod.configuration.ConfigurationBuilder createHotRodClientConfigurationBuilder(int serverPort) {
      return super.createHotRodClientConfigurationBuilder(serverPort).pingOnStartup(true);
   }

}
