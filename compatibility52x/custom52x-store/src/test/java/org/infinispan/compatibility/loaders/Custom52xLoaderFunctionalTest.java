package org.infinispan.compatibility.loaders;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;

/**
 * // TODO: Document this
 *
 * @author Mircea Markus
 * @since 6.0
 */
@Test (groups = "functional", testName = "compatibility.loaders.Custom52xLoaderFunctionalTest")
public class Custom52xLoaderFunctionalTest extends BaseCacheStoreFunctionalTest {
   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
      new File(tmpDirectory).mkdirs();
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
   }

   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      Custom52xCacheStoreConfig cfg = new Custom52xCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }

}
