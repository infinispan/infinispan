package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * Verifies the Query DSL functionality for Filesystem directory_provider.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.dsl.embedded.FilesystemQueryDslConditionsTest")
@CleanupAfterMethod
public class FilesystemQueryDslConditionsTest extends QueryDslConditionsTest {

   private final String indexDirectory = TestingUtil.tmpDirectory(this.getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      TestingUtil.recursiveFileRemove(indexDirectory);
      boolean created = new File(indexDirectory).mkdirs();
      assertTrue(created);
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         //first stop cache managers, then clear the index
         super.teardown();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
