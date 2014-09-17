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
@Test(testName = "client.hotrod.BulkGetReplTest", groups = "functional")
public class BulkGetReplTest extends BaseBulkGetTest {

   @Override
   protected int numberOfHotRodServers() {
      return 3;
   }

   @Override
   protected ConfigurationBuilder clusterConfig() {
      return getDefaultClusteredCacheConfig(
            CacheMode.REPL_SYNC, true);
   }
}
