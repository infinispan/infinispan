package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 *
 * @author <a href="mailto:rtsang@redhat.com">Ray Tsang</a>
 * @since 5.2
 */
@Test(testName = "client.hotrod.BulkGetKeysSimpleTest", groups = "functional")
public class BulkGetKeysSimpleTest extends BaseBulkGetKeysTest {

   @Override
   protected int numberOfHotRodServers() {
      return 1;
   }

   @Override
   protected ConfigurationBuilder clusterConfig() {
      return getDefaultClusteredCacheConfig(
            CacheMode.LOCAL, true);
   }

}
