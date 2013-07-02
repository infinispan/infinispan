package org.infinispan.loaders.jdbm;

import java.io.File;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.jdbm.JdbmCacheStoreFunctionalTest")
public class JdbmCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

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
      JdbmCacheStoreConfig cfg = new JdbmCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true); // for more accurate unit testing
      return cfg;
   }

}
