package org.infinispan.persistence.leveldb;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfigurationBuilder;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.leveldb.JavaLevelDBCacheStoreTest")
public class JavaLevelDBCacheStoreTest extends LevelDBStoreTest {

   protected LevelDBStoreConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder lcb) {
      LevelDBStoreConfigurationBuilder builder = super.createCacheStoreConfig(lcb);
      builder.implementationType(LevelDBStoreConfiguration.ImplementationType.JAVA);
      return builder;
   }
}
