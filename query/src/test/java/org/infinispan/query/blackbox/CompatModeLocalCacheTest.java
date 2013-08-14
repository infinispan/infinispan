package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Verify queries in compatibility mode.
 *
 * @author Martin Gencur
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.blackbox.CompatModeLocalCacheTest")
public class CompatModeLocalCacheTest extends LocalCacheTest {

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .compatibility().enable()
            .indexing()
            .enable()
            .addProperty("default.directory_provider", "ram")
            .addProperty("lucene_version", "LUCENE_CURRENT");
      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }
}