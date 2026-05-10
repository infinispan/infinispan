package org.infinispan.client.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import org.infinispan.configuration.cache.CacheMode;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.SegmentOwnershipDistTest")
public class SegmentOwnershipDistTest extends BaseSegmentOwnershipTest {

   public void testObtainSegmentOwnership() throws Exception {
      RemoteCache<Object, Object> remoteCache = client(0).getCache();
      Map<SocketAddress, Set<Integer>> segmentsByServer = remoteCache.getCacheTopologyInfo().getSegmentsPerServer();
      Map<Integer, Set<SocketAddress>> serversBySegment = invertMap(segmentsByServer);

      assertEquals(NUM_SERVERS, segmentsByServer.keySet().size());
      assertTrue(serversBySegment.entrySet().stream().allMatch(e -> e.getValue().size() == NUM_OWNERS));
   }

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.DIST_SYNC;
   }
}
