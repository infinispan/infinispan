package org.infinispan.persistence.factory;

import org.infinispan.commons.CacheException;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.factory.configuration.MyCustomStoreConfiguration;
import org.testng.annotations.Test;

import java.net.URL;
import java.net.URLClassLoader;

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

   public void testIfInstanceFromDifferentClassLoaderIsReturned() throws Exception {
      // given
      final Object instanceReturnedByTheFactory = loadWithCustomClassLoader(this.getClass().getName());
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

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000332: Unable to instantiate class for StoreConfiguration org.infinispan.persistence.factory.configuration.MyCustomStoreConfiguration.*")
   public void testIfACacheExceptionIfThrownWhenNoInstanceIfFound() throws Exception {
      // given
      StoreConfiguration configuration = new MyCustomStoreConfiguration();
      CacheStoreFactoryRegistry registry = new CacheStoreFactoryRegistry();

      // when
      registry.clearFactories();
      registry.createInstance(configuration);
   }

   private Object loadWithCustomClassLoader(String className) throws Exception {
      URL thisClass = this.getClass().getResource(this.getClass().getSimpleName() + ".class");
      ClassLoader customClassLoader = new URLClassLoader(new URL[] {thisClass});
      Class<?> customInstanceFromDifferentClassLoader = customClassLoader.loadClass(className);
      return customInstanceFromDifferentClassLoader.newInstance();
   }
}