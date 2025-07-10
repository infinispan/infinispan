package org.infinispan.persistence.rocksdb;

import java.io.File;

import org.infinispan.commons.test.CommonsTestingUtil;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.persistence.ParallelIterationTest;
import org.infinispan.persistence.rocksdb.configuration.RocksDBStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test (groups = {"functional", "smoke"}, testName = "persistence.rocksdb.RocksDBParallelIterationTest")
public class RocksDBParallelIterationTest extends ParallelIterationTest {

   private String tmpDirectory;

   @Override
   protected void configurePersistence(ConfigurationBuilder cb) {
      tmpDirectory = CommonsTestingUtil.tmpDirectory(this.getClass());
      new File(tmpDirectory).mkdirs();
      cb.persistence()
            .addStore(RocksDBStoreConfigurationBuilder.class)
            .location(tmpDirectory + "/data")
            .expiredLocation(tmpDirectory + "/expiry");
   }

   @Override
   protected void teardown() {
      Util.recursiveFileRemove(tmpDirectory);
      super.teardown();
   }

}
