package org.infinispan.loaders.jdbm;

import java.io.File;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.jdbm.configuration.JdbmCacheStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
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
   protected LoadersConfigurationBuilder createCacheStoreConfig(LoadersConfigurationBuilder loaders) {
      loaders
         .addStore(JdbmCacheStoreConfigurationBuilder.class)
            .location(tmpDirectory)
            .purgeSynchronously(true);
      return loaders;
   }

}
