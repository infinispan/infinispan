package org.infinispan.server.hotrod.event;

import static org.infinispan.server.hotrod.OperationStatus.ServerError;
import static org.infinispan.server.hotrod.OperationStatus.Success;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.assertStatus;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;
import static org.infinispan.server.hotrod.test.HotRodTestingUtil.startHotRodServer;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.HotRodMultiNodeTest;
import org.infinispan.server.hotrod.HotRodServer;
import org.infinispan.server.hotrod.configuration.HotRodServerConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Verifies that a clustered server applies the connector's {@code max-bloom-filter-bits} limit to primary near cache
 * listener registrations: a registration at the limit is accepted and one above it is rejected.
 */
@Test(groups = "functional", testName = "server.hotrod.event.HotRodPrimaryBloomFilterLimitTest")
public class HotRodPrimaryBloomFilterLimitTest extends HotRodMultiNodeTest {

   private static final int MAX_BLOOM_BITS = 1024;

   @Override
   protected String cacheName() {
      return "primaryBloomFilterLimitCache";
   }

   @Override
   protected ConfigurationBuilder createCacheConfig() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }

   @Override
   protected HotRodServer startTestHotRodServer(EmbeddedCacheManager cacheManager, int port) {
      HotRodServerConfigurationBuilder builder = new HotRodServerConfigurationBuilder();
      builder.maxBloomFilterBits(MAX_BLOOM_BITS);
      return startHotRodServer(cacheManager, port, builder);
   }

   public void testPrimaryListenerAtLimitAccepted() {
      EventLogListener listener = new EventLogListener();
      assertStatus(clients().get(0).addPrimaryNearCacheListener(listener, MAX_BLOOM_BITS), Success);
      assertStatus(clients().get(0).removeClientListener(listener.getId()), Success);
   }

   public void testPrimaryListenerOverLimitRejected() {
      assertStatus(clients().get(0).addPrimaryNearCacheListener(new EventLogListener(), MAX_BLOOM_BITS + 1), ServerError);
   }
}
