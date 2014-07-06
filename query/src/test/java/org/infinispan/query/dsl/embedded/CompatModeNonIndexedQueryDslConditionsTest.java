package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
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
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.compatibility().enable()
            .transaction().transactionMode(TransactionMode.TRANSACTIONAL);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}
