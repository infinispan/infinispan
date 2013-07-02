package org.infinispan.loaders.bdbje;

import java.io.File;

import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.CacheStoreConfig;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test(groups = "unit", enabled = true, testName = "loaders.bdbje.BdbjeCacheStoreFunctionalIntegrationTest")
public class BdbjeCacheStoreFunctionalIntegrationTest extends BaseCacheStoreFunctionalTest {

   private String tmpDirectory;

   @BeforeTest
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
      BdbjeCacheStoreConfig cfg = new BdbjeCacheStoreConfig();
      cfg.setLocation(tmpDirectory);
      cfg.setPurgeSynchronously(true);
      return cfg;
   }

}
