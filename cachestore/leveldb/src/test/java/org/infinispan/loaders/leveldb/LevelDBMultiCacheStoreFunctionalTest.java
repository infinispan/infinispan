package org.infinispan.loaders.leveldb;

import java.io.File;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.MultiCacheStoreFunctionalTest;
import org.infinispan.loaders.leveldb.configuration.LevelDBCacheStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "loaders.leveldb.LevelDBMultiCacheStoreFunctionalTest")
public class LevelDBMultiCacheStoreFunctionalTest extends MultiCacheStoreFunctionalTest<LevelDBCacheStoreConfigurationBuilder> {

   private File tmpDir;

   @BeforeClass
   protected void setPaths() {
      tmpDir = new File(TestingUtil.tmpDirectory(this));
   }

   @BeforeMethod
   protected void cleanDataFiles() {
      if (tmpDir.exists()) {
         TestingUtil.recursiveFileRemove(tmpDir);
      }
   }

   @Override
   protected LevelDBCacheStoreConfigurationBuilder buildCacheStoreConfig(LoadersConfigurationBuilder loaders, String discriminator) {
      LevelDBCacheStoreConfigurationBuilder store = loaders.addStore(LevelDBCacheStoreConfigurationBuilder.class);
      store.location(tmpDir.getAbsolutePath() + File.separator + "leveldb" + File.separator + "data-" + discriminator);
      store.expiredLocation(tmpDir.getAbsolutePath() + File.separator + "leveldb" + File.separator + "expired-data-" + discriminator);
      store.implementationType(LevelDBCacheStoreConfig.ImplementationType.JAVA);
      return store;
   }
}
