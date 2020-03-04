package org.infinispan.query.dsl.embedded;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

/**
 * Verifies the Query DSL functionality for Filesystem directory_provider.
 *
 * @author Anna Manukyan
 * @author anistor@redhat.com
 * @since 6.0
 */
@Test(groups = "functional", testName = "query.dsl.embedded.FilesystemQueryDslConditionsTest")
public class FilesystemQueryDslConditionsTest extends QueryDslConditionsTest {

   private final String indexDirectory = CommonsTestingUtil.tmpDirectory(getClass());

   @Override
   protected void createCacheManagers() throws Throwable {
      Util.recursiveFileRemove(indexDirectory);
      boolean created = new File(indexDirectory).mkdirs();
      assertTrue(created);

      ConfigurationBuilder cfg = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      cfg.indexing().index(Index.ALL)
            .addIndexedEntity(getModelFactory().getUserImplClass())
            .addIndexedEntity(getModelFactory().getAccountImplClass())
            .addIndexedEntity(getModelFactory().getTransactionImplClass())
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      createClusteredCaches(1, cfg);
   }

   @AfterClass
   @Override
   protected void destroy() {
      try {
         //first stop cache managers, then clear the index
         super.destroy();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         Util.recursiveFileRemove(indexDirectory);
      }
   }
}
