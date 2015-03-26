package org.infinispan.persistence.factory;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.StoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
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
         public <T> T createInstance(StoreConfiguration storeConfiguration) {
            return (T) instanceReturnedByTheFactory;
         }

         @Override
         public StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration) {
            return null;
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
         public <T> T createInstance(StoreConfiguration storeConfiguration) {
            return (T) instanceReturnedByTheFactory;
         }

         @Override
         public StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration) {
            return null;
         }
      });

      // when
      Object instance = registry.createInstance(doesNotMatter);

      // then
      assertEquals(instance, instanceReturnedByTheFactory);
   }

   @Test(expectedExceptions = CacheException.class, expectedExceptionsMessageRegExp = "ISPN000\\d{3}: Unable to instantiate loader/writer instance for StoreConfiguration class .*.DummyInMemoryStoreConfiguration")
   public void testIfACacheExceptionIfThrownWhenNoInstanceIfFound() throws Exception {
      // given
      StoreConfiguration configuration = createDummyConfiguration();
      CacheStoreFactoryRegistry registry = new CacheStoreFactoryRegistry();

      // when
      registry.clearFactories();
      registry.createInstance(configuration);
   }

   @Test
   public void testProcessingConfigurationWithoutCustomFactories () throws Exception {
      // given
      StoreConfiguration configuration = createDummyConfiguration();
      CacheStoreFactoryRegistry registry = new CacheStoreFactoryRegistry();

      //when
      StoreConfiguration processedConfiguration = registry.processStoreConfiguration(configuration);

      //than
      assertEquals(configuration, processedConfiguration);
   }

   @Test
   public void testCustomFactoryProcessesConfigurationFirst() throws Exception {
      // given
      final StoreConfiguration configuration = createDummyConfiguration();
      final StoreConfiguration enhancedConfiguration = createDummyConfiguration();

      CacheStoreFactoryRegistry registry = new CacheStoreFactoryRegistry();
      registry.addCacheStoreFactory(new CacheStoreFactory() {

         @Override
         public <T> T createInstance(StoreConfiguration storeConfiguration) {
            throw new AssertionError("Should not be called");
         }

         @Override
         public StoreConfiguration processConfiguration(StoreConfiguration storeConfiguration) {
            return enhancedConfiguration;
         }
      });

      // when
      StoreConfiguration returnedConfiguration = registry.processStoreConfiguration(configuration);

      // then
      assertEquals(enhancedConfiguration, returnedConfiguration);
   }

   private DummyInMemoryStoreConfiguration createDummyConfiguration() {
      AttributeSet protectedAttributesSet = DummyInMemoryStoreConfiguration.attributeDefinitionSet().protect();
      return new DummyInMemoryStoreConfiguration(protectedAttributesSet, null, null);
   }

   private Object loadWithCustomClassLoader(String className) throws Exception {
      URL thisClass = this.getClass().getResource(this.getClass().getSimpleName() + ".class");
      ClassLoader customClassLoader = new URLClassLoader(new URL[] {thisClass});
      Class<?> customInstanceFromDifferentClassLoader = customClassLoader.loadClass(className);
      return customInstanceFromDifferentClassLoader.newInstance();
   }
}