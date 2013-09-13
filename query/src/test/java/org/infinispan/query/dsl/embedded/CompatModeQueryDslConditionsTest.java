package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.Test;

/**
 * Verify query DSL in compatibility mode.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.CompatModeQueryDslConditionsTest")
public class CompatModeQueryDslConditionsTest extends QueryDslConditionsTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.compatibility().enable()
            .transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

}
