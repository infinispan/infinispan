package org.infinispan.loaders.file;

import java.io.File;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.file.FileCacheStoreFunctionalTest")
public class FileCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }
   
   @AfterClass
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }
   
   @Override
   protected CacheStoreConfig createCacheStoreConfig() throws Exception {
      FileCacheStoreConfig cfg = new FileCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }

}
