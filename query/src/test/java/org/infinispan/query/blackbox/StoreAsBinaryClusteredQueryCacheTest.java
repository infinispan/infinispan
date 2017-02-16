package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Tests for Clustered distributed queries using marshalled values in cache.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.StoreAsBinaryClusteredQueryCacheTest")
public class StoreAsBinaryClusteredQueryCacheTest extends ClusteredQueryTest {

   protected void enhanceConfig(ConfigurationBuilder cacheCfg) {
      cacheCfg.storeAsBinary().enabled(true);
   }
}
