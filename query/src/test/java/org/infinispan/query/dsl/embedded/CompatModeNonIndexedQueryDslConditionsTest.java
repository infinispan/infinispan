package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify non-indexed query in compatibility mode.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
@Test(groups = "functional", testName = "query.dsl.CompatModeNonIndexedQueryDslConditionsTest")
public class CompatModeNonIndexedQueryDslConditionsTest extends NonIndexedQueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.compatibility().enable();
      createClusteredCaches(1, cfg);
   }
}
