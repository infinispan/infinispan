package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.testng.annotations.Test;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.NonIndexedClusteredQueryDslConditionsTest")
public class NonIndexedClusteredQueryDslConditionsTest extends NonIndexedQueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder defaultConfiguration = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, false);
      defaultConfiguration.clustering()
            .stateTransfer().fetchInMemoryState(true);
      createClusteredCaches(2, defaultConfiguration);
   }
}
