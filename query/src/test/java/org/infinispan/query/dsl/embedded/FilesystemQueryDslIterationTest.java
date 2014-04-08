package org.infinispan.query.dsl.embedded;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

/**
 * Verifies the functionality of DSL iterators for Filesystem directory provider.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.dsl.embedded.FilesystemQueryDslIterationTest")
@CleanupAfterMethod
public class FilesystemQueryDslIterationTest extends QueryDslIterationTest {

   private final String indexDirectory = TestingUtil.tmpDirectory(getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeClass(alwaysRun = true)
   protected void setUp() throws Exception {
      TestingUtil.recursiveFileRemove(indexDirectory);
      new File(indexDirectory).mkdirs();
   }

   @AfterClass(alwaysRun = true)
   protected void tearDown() {
      TestingUtil.recursiveFileRemove(indexDirectory);
   }
}
