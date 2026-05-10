package org.infinispan.client.hotrod;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.SegmentOwnershipLocalTest")
public class SegmentOwnershipLocalTest extends SingleHotRodServerTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      return super.createCacheManager();
   }

   @Test
   public void testSegmentMap() throws Exception {
      RemoteCache<Object, Object> cache = remoteCacheManager.getCache();

      Map<SocketAddress, Set<Integer>> segmentsByServer = cache.getCacheTopologyInfo().getSegmentsPerServer();

      assertNotNull(segmentsByServer);
      assertEquals(1, segmentsByServer.keySet().size());
      assertEquals(0, segmentsByServer.values().iterator().next().size());
   }
}
