package org.infinispan.persistence.factory;

import org.infinispan.configuration.cache.StoreConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;

@Test(groups = "unit", testName = "persistence.CacheStoreFactoryRegistryTest")
public class CacheStoreFactoryRegistryTest {

   public void testIfNewlyAddedFactoryIsInvokedFirst() {
      // given
      final Object instanceReturnedByTheFactory = new Object();
      StoreConfiguration doesNotMatter = null;

      CacheStoreFactoryRegistry registry = new CacheStoreFactoryRegistry();
      registry.addCacheStoreFactory(new CacheStoreFactory() {

         @Override
         public Object createInstance(StoreConfiguration storeConfiguration) {
            return instanceReturnedByTheFactory;
         }
      });

      // when
      Object instance = registry.createInstance(doesNotMatter);

      // then
      assertEquals(instance, instanceReturnedByTheFactory);
   }

}