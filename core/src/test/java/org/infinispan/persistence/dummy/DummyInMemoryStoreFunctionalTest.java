package org.infinispan.persistence.dummy;

import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.BaseCacheStoreFunctionalTest;
import org.infinispan.persistence.CacheLoaderException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

@Test(groups = "unit", testName = "loaders.dummy.DummyInMemoryStoreFunctionalTest")
public class DummyInMemoryStoreFunctionalTest extends BaseCacheStoreFunctionalTest {

   @AfterClass
   protected void clearTempDir() {
      DummyInMemoryStore.stores.remove(getClass().getName());
   }

   @Override
   protected PersistenceConfigurationBuilder createCacheStoreConfig(PersistenceConfigurationBuilder loaders, boolean preload) {
      loaders
         .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .storeName(getClass().getName())
            .purgeOnStartup(false).preload(preload);
      return loaders;
   }

   @Override
   public void testStoreByteArrays(Method m) throws CacheLoaderException {
      super.testStoreByteArrays(m);    // TODO: Customise this generated block
   }
}
