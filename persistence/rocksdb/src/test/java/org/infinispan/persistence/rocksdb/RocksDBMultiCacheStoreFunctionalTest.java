package org.infinispan.persistence.rocksdb;

import java.io.File;
import java.nio.file.Paths;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.MultiStoresFunctionalTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.rocksdb.RocksDBMultiCacheStoreFunctionalTest")
public class RocksDBMultiCacheStoreFunctionalTest extends MultiStoresFunctionalTest<RocksDBStoreConfigurationBuilder> {

   private File tmpDir = new File(CommonsTestingUtil.tmpDirectory(this.getClass()));

   @BeforeMethod
   protected void cleanDataFiles() {
      if (tmpDir.exists()) {
         Util.recursiveFileRemove(tmpDir);
      }
   }

   @Override
   protected RocksDBStoreConfigurationBuilder buildCacheStoreConfig(PersistenceConfigurationBuilder p, String discriminator) {
      RocksDBStoreConfigurationBuilder store = p.addStore(RocksDBStoreConfigurationBuilder.class);
      store.location(Paths.get(tmpDir.getAbsolutePath(), "rocksdb", "data" + discriminator).toString());
      store.expiredLocation(Paths.get(tmpDir.getAbsolutePath(), "rocksdb", "expired-data-" + discriminator).toString());
      return store;
   }
}
