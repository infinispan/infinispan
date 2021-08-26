package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreFunctionalTest")
public class DummyInMemoryStoreFunctionalTest extends BaseStoreFunctionalTest {
   @AfterClass
   protected void clearTempDir() {
      DummyInMemoryStore.removeStoreData(getClass().getName());
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence,
         String cacheName, boolean preload) {
      persistence
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(false).preload(preload);
      return persistence;
   }
}
