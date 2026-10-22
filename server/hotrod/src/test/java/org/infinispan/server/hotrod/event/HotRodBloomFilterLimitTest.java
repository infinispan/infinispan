package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.OperationStatus.ServerError;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.HotRodSingleNodeTest;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Verifies that the connector's {@code max-bloom-filter-bits} limit applies to both the cluster-wide and the
 * primary near cache listener registrations. Accepting a primary registration requires a clustered server, which
 * {@link HotRodPrimaryBloomFilterLimitTest} covers.
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodBloomFilterLimitTest")
public class HotRodBloomFilterLimitTest extends HotRodSingleNodeTest {

   private static final int MAX_BLOOM_BITS = 1024;

   @Override
   protected HotRodServer createStartHotRodServer(EmbeddedCacheManager cacheManager) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.maxBloomFilterBits(MAX_BLOOM_BITS);
      return startHotRodServer(cacheManager, builder);
   }

   public void testClusterListenerOverLimitRejected() {
      assertStatus(client().addBloomNearCacheListener(new EventLogListener(), MAX_BLOOM_BITS + 1), ServerError);
   }

   public void testClusterListenerAtLimitAccepted() {
      EventLogListener listener = new EventLogListener();
      assertStatus(client().addBloomNearCacheListener(listener, MAX_BLOOM_BITS), Success);
      assertStatus(client().removeClientListener(listener.getId()), Success);
   }

}
