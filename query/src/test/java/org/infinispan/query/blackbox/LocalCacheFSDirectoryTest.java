package org.infinispan.query.blackbox;

import static org.testng.AssertJUnit.assertTrue;

import java.io.File;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.IndexStorage;
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
 * Run the basic set of operations with filesystem-based index storage.
 * The default FSDirectory implementation for non Windows systems should be NIOFSDirectory.
 * SimpleFSDirectory implementation will be used on Windows.
 *
 * @author Martin Gencur
 */
@Test(groups = "functional", testName = "query.blackbox.LocalCacheFSDirectoryTest")
public class LocalCacheFSDirectoryTest extends LocalCacheTest {

   private final String indexDirectory = CommonsTestingUtil.tmpDirectory(getClass());

   @Override
   protected EmbeddedCacheManager createCacheManager() throws Exception {
      ConfigurationBuilder cfg = getDefaultStandaloneCacheConfig(true);
      cfg.indexing().enable()
            .storage(IndexStorage.FILESYSTEM).path(indexDirectory)
            .addIndexedEntity(Person.class)
            .addIndexedEntity(AnotherGrassEater.class)
            .addKeyTransformer(CustomKey3.class, CustomKey3Transformer.class)
            .addProperty(SearchConfig.ERROR_HANDLER, StaticTestingErrorHandler.class.getName());
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
