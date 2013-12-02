package org.infinispan.persistence.leveldb;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.infinispan.persistence.MultiStoresFunctionalTest;
import org.infinispan.test.TestingUtil;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;

@Test(groups = "unit", testName = "persistence.leveldb.LevelDBMultiCacheStoreFunctionalTest")
public class LevelDBMultiCacheStoreFunctionalTest extends MultiStoresFunctionalTest<LevelDBStoreConfigurationBuilder> {

   private File tmpDir;

   @BeforeClass
   protected void setPaths() {
      tmpDir = new File(TestingUtil.tmpDirectory(this.getClass()));
   }

   @BeforeMethod
   protected void cleanDataFiles() {
      if (tmpDir.exists()) {
         TestingUtil.recursiveFileRemove(tmpDir);
      }
   }


   @Override
   protected LevelDBStoreConfigurationBuilder buildCacheStoreConfig(PersistenceConfigurationBuilder p, String discriminator) throws Exception {
      LevelDBStoreConfigurationBuilder store = p.addStore(LevelDBStoreConfigurationBuilder.class);
      store.location(tmpDir.getAbsolutePath() + File.separator + "leveldb" + File.separator + "data-" + discriminator);
      store.expiredLocation(tmpDir.getAbsolutePath() + File.separator + "leveldb" + File.separator + "expired-data-" + discriminator);
      store.implementationType(LevelDBStoreConfiguration.ImplementationType.JAVA);
      return store;
   }
}
