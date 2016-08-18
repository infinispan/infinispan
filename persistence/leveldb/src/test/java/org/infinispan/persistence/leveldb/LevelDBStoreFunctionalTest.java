package org.infinispan.persistence.leveldb;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;

import java.io.File;

public abstract class LevelDBStoreFunctionalTest extends BaseStoreFunctionalTest {
   protected String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   LevelDBStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      new File(tmpDirectory).mkdirs();
      return loaders.addStore(LevelDBStoreConfigurationBuilder.class).location(tmpDirectory + "/data").expiredLocation(tmpDirectory + "/expiry").clearThreshold(2);
   }
}
