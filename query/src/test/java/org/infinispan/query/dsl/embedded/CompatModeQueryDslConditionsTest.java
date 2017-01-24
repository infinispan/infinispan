package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify query DSL in compatibility mode. Just a smoke test that executes in embedded mode and no interaction is done
 * via a remote client. This just ensures nothing gets broken on the embedded side if compatibility is active.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.CompatModeQueryDslConditionsTest")
public class CompatModeQueryDslConditionsTest extends QueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.compatibility().enable()
            .indexing().index(Index.ALL)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass())
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }
}
