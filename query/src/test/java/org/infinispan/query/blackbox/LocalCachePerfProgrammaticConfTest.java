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
 * Testing the tuned options with programmatic configuration.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerfProgrammaticConfTest")
public class LocalCachePerfProgrammaticConfTest extends LocalCacheTest {

   private String indexDirectory;
   private String directoryName = "/perfProgrammaticDir";

   public LocalCachePerfProgrammaticConfTest() {
      indexDirectory = TestingUtil.tmpDirectory(this) + directoryName;
   }

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .enable()
            .indexLocalOnly(false)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.chunk_size", "128000")
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("default.exclusive_index_use", "true")
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "4096")
            .addProperty("default.indexwriter.ram_buffer_size", "220")
            .addProperty("default.locking_strategy", "native")
            .addProperty("default.sharding_strategy.nbr_of_shards", "6")
            .addProperty("lucene_version", "LUCENE_36");

      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @BeforeMethod
   protected void createDirectory() {
      new File(indexDirectory).mkdirs();
   }

   @Override
   @AfterMethod
   protected void destroyAfterMethod() {
      try {
         //first stop cache managers, then clear the index
         super.destroyAfterMethod();
      } finally {
         //delete the index otherwise it will mess up the index for next tests
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
