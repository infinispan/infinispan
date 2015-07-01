package org.infinispan.client.hotrod;

import org.infinispan.client.hotrod.test.SingleHotRodServerTest;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNotNull;

/**
 * @author gustavonalle
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.SegmentOwnershipDistTest")
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
      assertEquals(segmentsByServer.keySet().size(), 1);
      assertEquals(segmentsByServer.values().iterator().next().size(), 0);
   }
}