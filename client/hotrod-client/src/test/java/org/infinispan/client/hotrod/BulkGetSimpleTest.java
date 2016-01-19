package org.infinispan.client.hotrod;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests functionality related to getting multiple entries from a HotRod server
 * in bulk.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
@Test(testName = "client.hotrod.BulkGetSimpleTest", groups = "functional")
public class BulkGetSimpleTest extends BaseBulkGetTest {

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
