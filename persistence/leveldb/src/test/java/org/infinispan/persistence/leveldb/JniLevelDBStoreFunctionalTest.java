package org.infinispan.persistence.leveldb;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.leveldb.configuration.LevelDBStoreConfiguration;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.leveldb.JniLevelDBStoreFunctionalTest")
public class JniLevelDBStoreFunctionalTest extends LevelDBStoreFunctionalTest {

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder p, boolean preload) {
      super.createStoreBuilder(p)
            .preload(preload)
            .implementationType(LevelDBStoreConfiguration.ImplementationType.JNI);
      return p;
   }
}
