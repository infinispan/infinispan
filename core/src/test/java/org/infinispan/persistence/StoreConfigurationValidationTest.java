package org.infinispan.persistence;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.commons.persistence.Store;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.configuration.cache.SingletonStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.testng.annotations.Test;

/**
 * Unit test for ensuring that {@link AbstractStoreConfigurationBuilder#validate()} fails when expected.
 *
 * @author Ryan Emerson
 * @since 9.0
 */
@Test(groups = "unit", testName = "persistence.StoreConfigurationValidationTest")
public class StoreConfigurationValidationTest {

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".* NonSharedDummyInMemoryStore cannot be shared")
   public void testExceptionOnNonSharableStore() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(NonSharedDummyStoreConfigurationBuilder.class)
            .shared(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".* It is not possible for a store to be transactional in a non-transactional cache. ")
   public void testTxStoreInNonTxCache() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .transactional(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".* It is not possible for a store to be transactional when passivation is enabled. ")
   public void testTxStoreInPassivatedCache() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .transactional(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".* Cannot enable 'fetchPersistentState' in invalidation caches!")
   public void testFetchPersistentStateInInvalidationMode() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.clustering()
            .cacheMode(CacheMode.INVALIDATION_SYNC)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .fetchPersistentState(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = ".* A store cannot be shared when utilised with a local cache.")
   public void testSharedStoreWithLocalCache() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.clustering()
            .cacheMode(CacheMode.LOCAL)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .shared(true)
            .validate();
   }

   @Store
   @ConfiguredBy(NonSharedDummyStoreConfiguration.class)
   static class NonSharedDummyInMemoryStore extends DummyInMemoryStore {
      public NonSharedDummyInMemoryStore() {
         super();
      }
   }

   @BuiltBy(NonSharedDummyStoreConfigurationBuilder.class)
   @ConfigurationFor(NonSharedDummyInMemoryStore.class)
   static class NonSharedDummyStoreConfiguration extends DummyInMemoryStoreConfiguration {

      public static AttributeSet attributeDefinitionSet() {
         return new AttributeSet(NonSharedDummyStoreConfiguration.class, DummyInMemoryStoreConfiguration.attributeDefinitionSet());
      }

      NonSharedDummyStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async, SingletonStoreConfiguration singletonStore) {
         super(attributes, async, singletonStore);
      }
   }

   public static class NonSharedDummyStoreConfigurationBuilder
         extends AbstractStoreConfigurationBuilder<NonSharedDummyStoreConfiguration, NonSharedDummyStoreConfigurationBuilder> {

      public NonSharedDummyStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder, NonSharedDummyStoreConfiguration.attributeDefinitionSet());
      }

      @Override
      public NonSharedDummyStoreConfiguration create() {
         return new NonSharedDummyStoreConfiguration(attributes.protect(), async.create(), singletonStore.create());
      }

      @Override
      public NonSharedDummyStoreConfigurationBuilder self() {
         return this;
      }
   }
}
