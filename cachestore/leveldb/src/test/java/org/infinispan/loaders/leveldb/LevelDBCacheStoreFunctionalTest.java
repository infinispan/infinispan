package org.infinispan.loaders.leveldb;

import java.io.File;

import org.infinispan.configuration.cache.LoadersConfigurationBuilder;
import org.infinispan.loaders.BaseCacheStoreFunctionalTest;
import org.infinispan.loaders.leveldb.configuration.LevelDBCacheStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;

public abstract class LevelDBCacheStoreFunctionalTest extends BaseCacheStoreFunctionalTest {
   protected String tmpDirectory;

   @BeforeClass
   protected void setUpTempDir() {
      tmpDirectory = TestingUtil.tmpDirectory(this);
   }

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      TestingUtil.recursiveFileRemove(tmpDirectory);
      new File(tmpDirectory).mkdirs();
   }

   LevelDBCacheStoreConfigurationBuilder createStoreBuilder(LoadersConfigurationBuilder loaders) {
      return loaders.addStore(LevelDBCacheStoreConfigurationBuilder.class).location(tmpDirectory + "/data").expiredLocation(tmpDirectory + "/expiry").clearThreshold(2)
            .purgeSynchronously(true);
   }
}
