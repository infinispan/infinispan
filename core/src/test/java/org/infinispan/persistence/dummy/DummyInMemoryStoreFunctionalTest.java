package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseStoreFunctionalTest;
import org.infinispan.persistence.spi.PersistenceException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

@Test(groups = "unit", testName = "persistence.dummy.DummyInMemoryStoreFunctionalTest")
public class DummyInMemoryStoreFunctionalTest extends BaseStoreFunctionalTest {

   @AfterClass
   protected void clearTempDir() {
      DummyInMemoryStore.stores.remove(getClass().getName());
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder persistence, boolean preload) {
      persistence
         .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(false).preload(preload);
      return persistence;
   }

   @Override
   public void testStoreByteArrays(Method m) throws PersistenceException {
      super.testStoreByteArrays(m);    // TODO: Customise this generated block
   }
}
