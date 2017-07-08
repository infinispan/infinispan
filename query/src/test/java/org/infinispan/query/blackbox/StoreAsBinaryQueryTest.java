package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.StorageType;
import org.testng.annotations.Test;

/**
 * Test class such that it's possible to run queries on objects with the configuration setUseLazyDeserialization = true
 *
 * @author Navin Surtani
 * @since 4.0
 */
@Test(groups="functional", testName = "query.blackbox.StoreAsBinaryQueryTest")
public class StoreAsBinaryQueryTest extends LocalCacheTest {
   @Override
   protected void enhanceConfig(ConfigurationBuilder c) {
      c.memory().storageType(StorageType.BINARY);
   }
}
