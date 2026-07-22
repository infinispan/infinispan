package org.infinispan.persistence;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

import java.util.EnumSet;
import java.util.Set;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.configuration.BuiltBy;
import org.infinispan.commons.configuration.ConfigurationFor;
import org.infinispan.commons.configuration.ConfiguredBy;
import org.infinispan.commons.configuration.attributes.AttributeSet;
import org.infinispan.configuration.cache.AbstractStoreConfigurationBuilder;
import org.infinispan.configuration.cache.AsyncStoreConfiguration;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.ConfigurationBuilder;
import org.infinispan.configuration.cache.PersistenceConfigurationBuilder;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.testing.skip.StringLogAppender;
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
         expectedExceptionsMessageRegExp = "ISPN000549:.*")
   public void testExceptionOnNonSharableStore() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(NonSharedDummyStoreConfigurationBuilder.class)
            .shared(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN000417:.*")
   public void testTxStoreInNonTxCache() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .transactional(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN000418:.*")
   public void testTxStoreInPassivatedCache() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(true);
      builder.persistence()
            .passivation(true)
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .transactional(true)
            .validate();
   }

   @Test(expectedExceptions = CacheConfigurationException.class,
         expectedExceptionsMessageRegExp = "ISPN000549:.*")
   public void testSharedStoreWithLocalCache() {
      ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
      builder.clustering()
            .cacheMode(CacheMode.LOCAL)
            .persistence()
            .addStore(DummyInMemoryStoreConfigurationBuilder.class)
            .shared(true)
            .validate();
   }

   public void testWarningNonSharedStoreWithoutPurge() {
      Thread testThread = Thread.currentThread();
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> t == testThread,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
         builder.clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);
         builder.build();
         assertEquals(1, logAppender.size());
         assertTrue(logAppender.get(0).contains("ISPN000728"));
      } finally {
         logAppender.uninstall();
      }
   }

   public void testNoWarningNonSharedStoreWithoutPurgeLocalCache() {
      Thread testThread = Thread.currentThread();
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> t == testThread,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder builder = TestCacheManagerFactory.getDefaultCacheConfiguration(false);
         builder.persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);
         builder.build();
         assertEquals(0, logAppender.size());
      } finally {
         logAppender.uninstall();
      }
   }

   @ConfiguredBy(NonSharedDummyStoreConfiguration.class)
   static class NonSharedDummyInMemoryStore<K, V> extends DummyInMemoryStore<K, V> {
      public NonSharedDummyInMemoryStore() {
         super();
      }

      @Override
      public Set<Characteristic> characteristics() {
         return EnumSet.of(Characteristic.BULK_READ, Characteristic.EXPIRATION, Characteristic.SEGMENTABLE);
      }
   }

   @BuiltBy(NonSharedDummyStoreConfigurationBuilder.class)
   @ConfigurationFor(NonSharedDummyInMemoryStore.class)
   static class NonSharedDummyStoreConfiguration extends DummyInMemoryStoreConfiguration {

      public static AttributeSet attributeDefinitionSet() {
         return new AttributeSet(NonSharedDummyStoreConfiguration.class, DummyInMemoryStoreConfiguration.attributeDefinitionSet());
      }

      NonSharedDummyStoreConfiguration(AttributeSet attributes, AsyncStoreConfiguration async) {
         super(attributes, async);
      }
   }

   public static class NonSharedDummyStoreConfigurationBuilder
         extends AbstractStoreConfigurationBuilder<NonSharedDummyStoreConfiguration, NonSharedDummyStoreConfigurationBuilder> {

      public NonSharedDummyStoreConfigurationBuilder(PersistenceConfigurationBuilder builder) {
         super(builder, NonSharedDummyStoreConfiguration.attributeDefinitionSet());
      }

      @Override
      public NonSharedDummyStoreConfiguration create() {
         return new NonSharedDummyStoreConfiguration(attributes.protect(), async.create());
      }

      @Override
      public NonSharedDummyStoreConfigurationBuilder self() {
         return this;
      }
   }
}
