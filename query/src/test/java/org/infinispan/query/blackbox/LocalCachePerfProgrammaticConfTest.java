package org.infinispan.query.blackbox;

import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.Index;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Testing the tuned options with programmatic configuration.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerfProgrammaticConfTest")
public class LocalCachePerfProgrammaticConfTest extends LocalCacheTest {

   private final String indexDirectory = TestingUtil.tmpDirectory(this.getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .index(Index.ALL)
            .addProperty("default.directory_provider", "infinispan")
            .addProperty("default.chunk_size", "128000")
            .addProperty("default.indexmanager", "near-real-time")
            .addProperty("error_handler", "org.infinispan.query.helper.StaticTestingErrorHandler")
            .addProperty("default.indexBase", indexDirectory)
            .addProperty("default.indexwriter.merge_factor", "30")
            .addProperty("default.indexwriter.merge_max_size", "4096")
            .addProperty("default.indexwriter.ram_buffer_size", "220")
            .addProperty("default.locking_strategy", "native")
            .addProperty("default.sharding_strategy.nbr_of_shards", "6");

      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      new File(indexDirectory).mkdirs();
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         super.teardown();
      } finally {
         TestingUtil.recursiveFileRemove(indexDirectory);
      }
   }
}
