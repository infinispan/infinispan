package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 * Clustered version of {@link StoreAsBinaryQueryTest}
 *
 * @author Navin Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "query.blackbox.StoreAsBinaryClusteredQueryTest")
public class StoreAsBinaryClusteredQueryTest extends ClusteredCacheTest {

   @Override
   protected void enhanceConfig(ConfigurationBuilder c) {
      c.memory().storageType(StorageType.BINARY);
   }

}
