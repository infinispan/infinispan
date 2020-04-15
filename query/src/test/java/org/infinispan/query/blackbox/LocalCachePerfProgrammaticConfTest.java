package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.query.helper.SearchConfig;
import org.infinispan.query.helper.StaticTestingErrorHandler;
import org.infinispan.query.test.AnotherGrassEater;
import org.infinispan.query.test.CustomKey3;
import org.infinispan.query.test.CustomKey3Transformer;
import org.infinispan.query.test.Person;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Testing the tuned options with programmatic configuration.
 *
 * @author Anna Manukyan
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCachePerfProgrammaticConfTest")
public class LocalCachePerfProgrammaticConfTest extends LocalCacheTest {

   private final String indexDirectory = CommonsTestingUtil.tmpDirectory(getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg
            .indexing()
            .enable()
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addProperty(SearchConfig.DIRECTORY_TYPE, SearchConfig.HEAP)
            .addProperty(SearchConfig.DIRECTORY_ROOT, indexDirectory)
            .addProperty(SearchConfig.THREAD_POOL_SIZE, "6")
            .addProperty(SearchConfig.QUEUE_COUNT, "6")
            .addProperty(SearchConfig.QUEUE_SIZE, "4096")
            .addProperty(SearchConfig.COMMIT_INTERVAL, "10000")
            // changing the refresh interval would make the the test not working
            //.addProperty(SearchConfig.REFRESH_INTERVAL, "10000")
            .addProperty(SearchConfig.SHARDING_STRATEGY, SearchConfig.HASH)
            .addProperty(SearchConfig.NUMBER_OF_SHARDS, "6")
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());

      enhanceConfig(cfg);
      return TestCacheManagerFactory.createCacheManager(cfg);
   }

   @Override
   protected void setup() throws Exception {
      Util.recursiveFileRemove(indexDirectory);
      boolean created = new File(indexDirectory).mkdirs();
      assertTrue(created);
      super.setup();
   }

   @Override
   protected void teardown() {
      try {
         super.teardown();
      } finally {
         Util.recursiveFileRemove(indexDirectory);
      }
   }
}
