package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;
import java.nio.file.Paths;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.Person;
import org.infinispan.query.test.QueryTestSCI;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Run the basic set of operations with filesystem-based index storage in replicated mode
 * with transactional caches.
 *
 * The default FSDirectory implementation for non Windows systems should be NIOFSDirectory.
 * SimpleFSDirectory implementation will be used on Windows.
 *
 * @author Martin Gencur
 */
@Test(groups = "functional", testName = "query.blackbox.ClusteredCacheFSDirectoryTest")
public class ClusteredCacheFSDirectoryTest extends ClusteredCacheTest {

   private final String TMP_DIR = CommonsTestingUtil.tmpDirectory(getClass());

   @Override
   protected void createCacheManagers() {
      addClusterEnabledCacheManager(QueryTestSCI.INSTANCE, buildCacheConfig("index1"));
      addClusterEnabledCacheManager(QueryTestSCI.INSTANCE, buildCacheConfig("index2"));
      waitForClusterToForm();
      cache1 = cache(0);
      cache2 = cache(1);
   }

   private ConfigurationBuilder buildCacheConfig(String indexName) {
      ConfigurationBuilder cb = getDefaultClusteredCacheConfig(CacheMode.REPL_SYNC, true);
      cb.indexing()
            .enable() //index also changes originated on other nodes, the index is not shared
            .addIndexedEntity(Person.class)
            .addProperty("default.directory_provider", "filesystem")
            .addProperty("error_handler", StaticTestingErrorHandler.class.getName())
            .addProperty("default.indexBase", Paths.get(TMP_DIR,  indexName).toString())
            .addProperty("lucene_version", "LUCENE_CURRENT");
      return cb;
   }

   @BeforeClass
   protected void setUpTempDir() {
      Util.recursiveFileRemove(TMP_DIR);
      boolean created = new File(TMP_DIR).mkdirs();
      assertTrue(created);
   }

   @Override
   @AfterMethod
   protected void clearContent() throws Throwable {
      try {
         //first stop cache managers, then clear the index
         super.clearContent();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         Util.recursiveFileRemove(TMP_DIR);
      }
   }
}
