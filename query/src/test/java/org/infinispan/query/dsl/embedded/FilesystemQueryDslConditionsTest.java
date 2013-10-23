package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Verifies the Query DSL functionality for Filesystem directory_provider.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.dsl.embedded.FilesystemQueryDslConditionsTest")
@CleanupAfterMethod
public class FilesystemQueryDslConditionsTest extends QueryDslConditionsTest {

   private String indexDirectory;
   private String directoryName = "/dslConditionDir";

   public FilesystemQueryDslConditionsTest() {
      indexDirectory = TestingUtil.tmpDirectory(this) + directoryName;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.transaction()
            .transactionMode(TransactionMode.TRANSACTIONAL)
            .indexing().enable()
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod
   protected void populateCache() throws Exception {
      new File(indexDirectory).mkdirs();

      super.populateCache();
   }

   @Override
   @AfterMethod
   protected void clearContent(){
      try {
         //first stop cache managers, then clear the index
         super.clearContent();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
