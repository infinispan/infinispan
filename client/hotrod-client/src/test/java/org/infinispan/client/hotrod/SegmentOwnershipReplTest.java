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
@Test(groups = "functional", testName = "client.hotrod.SegmentOwnershipReplTest")
public class SegmentOwnershipReplTest extends BaseSegmentOwnershipTest {

   @Override
   protected CacheMode getCacheMode() {
      return CacheMode.REPL_SYNC;
   }

   @Test
   public void testObtainSegmentOwnership() throws Exception {
      RemoteCache<Object, Object> remoteCache = client(0).getCache();
      Map<SocketAddress, Set<Integer>> segmentsByServer = remoteCache.getCacheTopologyInfo().getSegmentsPerServer();

      assertEquals(NUM_SERVERS, segmentsByServer.keySet().size());
      assertTrue(segmentsByServer.entrySet().stream().allMatch(e -> e.getValue().size() == NUM_SEGMENTS));
   }

}
