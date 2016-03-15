package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Test class such that it's possible to run queries on objects with the configuration setUseLazyDeserialization = true
 *
 * @author Navin Surtani
 * @since 4.0
 */
@Test(groups="functional", testName = "query.blackbox.MarshalledValueQueryTest")
public class MarshalledValueQueryTest extends LocalCacheTest {
   @Override
   protected void enhanceConfig(ConfigurationBuilder c) {
      c.storeAsBinary().enabled(true);
   }
}
