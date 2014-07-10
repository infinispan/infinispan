package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

//todo This does not seem to actually test compatibility if no data is put via the remote client. I think EmbeddedCompatTest already tests this. need to check if other Compat test have this issue
/**
 * Verify query DSL in compatibility mode.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.CompatModeQueryDslConditionsTest")
public class CompatModeQueryDslConditionsTest extends QueryDslConditionsTest {

   @Override
   protected void createCacheManagers() throws Throwable {
      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.compatibility().enable()
            .indexing().index(Index.ALL)
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }
}
