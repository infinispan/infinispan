package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * Clustered version of {@link org.infinispan.query.blackbox.MarshalledValueQueryTest}
 *
 * @author Navin Surtani
 * @since 4.0
 */
@Test(groups = "functional", testName = "query.blackbox.MarshalledValueClusteredQueryTest")
public class MarshalledValueClusteredQueryTest extends ClusteredCacheTest {

   @Override
   protected void enhanceConfig(ConfigurationBuilder c) {
      c.storeAsBinary().enabled(true);
   }

}
