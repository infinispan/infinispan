package org.infinispan.client.hotrod.event;

import static org.infinispan.server.hotrod.test.HotRodTestingUtil.hotRodCacheConfiguration;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 8.0
 */
@Test(groups = "functional", testName = "client.hotrod.event.NonIndexedCacheRemoteListenerWithDslFilterTest")
public class NonIndexedCacheRemoteListenerWithDslFilterTest extends RemoteListenerWithDslFilterTest {

   @Override
   protected ConfigurationBuilder getConfigurationBuilder() {
      return hotRodCacheConfiguration(getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC, false));
   }
}
