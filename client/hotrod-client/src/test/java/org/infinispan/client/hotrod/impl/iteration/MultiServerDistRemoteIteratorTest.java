package org.infinispan.client.hotrod.impl.iteration;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.RemoteCacheManager;
import org.infinispan.client.hotrod.impl.consistenthash.SegmentConsistentHash;
import org.infinispan.client.hotrod.impl.transport.TransportFactory;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.test.TestingUtil;
import static org.testng.AssertJUnit.assertEquals;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.iteration.MultiServerDistRemoteIteratorTest")
public class MultiServerDistRemoteIteratorTest extends BaseMultiServerRemoteIteratorTest {

   private static final int NUM_SERVERS = 3;

   @Override
   protected void createCacheManagers() throws Throwable {
      createHotRodServers(NUM_SERVERS, getCacheConfiguration());
   }

   private ConfigurationBuilder getCacheConfiguration() {
      ConfigurationBuilder builder = getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false);
      builder.clustering().hash().numOwners(2);
      return builder;
   }

   @Test
   public void testIterationRouting() throws Exception {
      RemoteCacheManager cacheManager = clients.get(0);
      RemoteCache<Object, Object> remoteCache = cacheManager.getCache();
      TransportFactory transportFactory = TestingUtil.extractField(cacheManager, "transportFactory");
      SegmentConsistentHash consistentHash = (SegmentConsistentHash) transportFactory.getConsistentHash(cacheManager.getCache().getName().getBytes());
      SocketAddress[][] segmentOwners = consistentHash.getSegmentOwners();

      for (HotRodServer server : servers) {
         Set<Integer> segmentsOwned = getSegmentsOwned(server, segmentOwners);
         try (CloseableIterator<Map.Entry<Object, Object>> ignored = remoteCache.retrieveEntries(null, segmentsOwned, 10)) {
            assertEquals(1, server.iterationManager().activeIterations());
         }
      }
   }


   private Set<Integer> getSegmentsOwned(HotRodServer hotRodServer, SocketAddress[][] owners) {
      Set<Integer> owned = new HashSet<>();
      for (int seg = 0; seg < owners.length; seg++) {
         SocketAddress primaryOwner = owners[seg][0];
         InetSocketAddress inetSocketAddress = (InetSocketAddress) primaryOwner;
         String hostname = inetSocketAddress.getAddress().getHostAddress();
         int port = inetSocketAddress.getPort();
         if (hostname.equals(hotRodServer.getHost()) && port == hotRodServer.getPort()) {
            owned.add(seg);
         }
      }
      return owned;
   }

}
