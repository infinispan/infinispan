package org.infinispan.persistence.rocksdb;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBStoreFunctionalTest")
public class RocksDBStoreFunctionalTest extends BaseStoreFunctionalTest {
   protected String tmpDirectory = TestingUtil.tmpDirectory(this.getClass());

   @AfterClass(alwaysRun = true)
   protected void clearTempDir() {
      Util.recursiveFileRemove(tmpDirectory);
   }

   RocksDBStoreConfigurationBuilder createStoreBuilder(PersistenceConfigurationBuilder loaders) {
      new File(tmpDirectory).mkdirs();
      return loaders.addStore(RocksDBStoreConfigurationBuilder.class).location(tmpDirectory + "/data").expiredLocation(tmpDirectory + "/expiry").clearThreshold(2);
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      createStoreBuilder(persistence)
            .preload(preload);
      return persistence;
   }
}
