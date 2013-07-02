package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.io.File;

/**
 * Run the basic set of operations with filesystem-based index storage.
 * The default FSDirectory implementation for non Windows systems should be NIOFSDirectory.
 * SimpleFSDirectory implementation will be used on Windows.
 *
 * @author Martin Gencur
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCacheFSDirectoryTest")
public class LocalCacheFSDirectoryTest extends LocalCacheTest {

   private String TMP_DIR;

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
         .indexLocalOnly(false) //not meaningful
         .addProperty("default.directory_provider", "filesystem")
         .addProperty("default.indexBase", TMP_DIR + File.separator + "index")
         .addProperty("default.lucene_version", "LUCENE_CURRENT");
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod
   protected void setUpTempDir() {
      TMP_DIR = TestingUtil.tmpDirectory(this);
      new File(TMP_DIR).mkdirs();
   }

   @Override
   @AfterMethod
   protected void destroyAfterMethod() {
      try {
         //first stop cache managers, then clear the index
         super.destroyAfterMethod();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(TMP_DIR);
      }
   }
}
