package org.infinispan.persistence.rocksdb;

import java.io.File;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.MultiStoresFunctionalTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBMultiCacheStoreFunctionalTest")
public class RocksDBMultiCacheStoreFunctionalTest extends MultiStoresFunctionalTest<RocksDBStoreConfigurationBuilder> {

   private File tmpDir = new File(TestingUtil.tmpDirectory(this.getClass()));

   @BeforeMethod
   protected void cleanDataFiles() {
      if (tmpDir.exists()) {
         Util.recursiveFileRemove(tmpDir);
      }
   }

   @Override
   protected RocksDBStoreConfigurationBuilder buildCacheStoreConfig(PersistenceConfigurationBuilder p, String discriminator) throws Exception {
      RocksDBStoreConfigurationBuilder store = p.addStore(RocksDBStoreConfigurationBuilder.class);
      store.location(tmpDir.getAbsolutePath() + File.separator + "rocksdb" + File.separator + "data-" + discriminator);
      store.expiredLocation(tmpDir.getAbsolutePath() + File.separator + "rocksdb" + File.separator + "expired-data-" + discriminator);
      return store;
   }
}
