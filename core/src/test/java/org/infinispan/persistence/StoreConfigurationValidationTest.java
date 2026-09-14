package org.infinispan.persistence;

import static org.infinispan.test.TestingUtil.killCacheManagers;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.EnumSet;
import java.util.Set;
import java.util.stream.StreamSupport;

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
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.persistence.dummy.DummyInMemoryStore;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfiguration;
import org.infinispan.persistence.dummy.DummyInMemoryStoreConfigurationBuilder;
import org.infinispan.registry.InternalCacheRegistry;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
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
public class StoreConfigurationValidationTest extends AbstractInfinispanTest {

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

   @Test(groups = "functional")
   public void testWarningNonSharedStoreWithoutPurge() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager(builder);
         try {
            cm.getCache();
            long warnCount = StreamSupport.stream(logAppender.spliterator(), false)
                  .filter(s -> s.contains("ISPN000728")).count();
            assertEquals(1, warnCount);
         } finally {
            killCacheManagers(cm);
         }
      } finally {
         logAppender.uninstall();
      }
   }

   @Test(groups = "functional")
   public void testNoWarningNonSharedStoreWithoutPurgeLocalCache() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder builder = new ConfigurationBuilder();
         builder.persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createCacheManager(builder);
         try {
            cm.getCache();
            long warnCount = StreamSupport.stream(logAppender.spliterator(), false)
                  .filter(s -> s.contains("ISPN000728")).count();
            assertEquals(0, warnCount);
         } finally {
            killCacheManagers(cm);
         }
      } finally {
         logAppender.uninstall();
      }
   }

   @Test(groups = "functional")
   public void testNoWarningNonSharedStoreWithoutPurgeInternalCache() {
      StringLogAppender logAppender = new StringLogAppender("org.infinispan.CONFIG",
            Level.WARN,
            t -> true,
            PatternLayout.newBuilder().setPattern("%m").build());
      logAppender.install();
      try {
         ConfigurationBuilder internalCacheConfig = new ConfigurationBuilder();
         internalCacheConfig.clustering()
               .cacheMode(CacheMode.DIST_SYNC)
               .persistence()
               .addStore(DummyInMemoryStoreConfigurationBuilder.class);
         EmbeddedCacheManager cm = TestCacheManagerFactory.createClusteredCacheManager();
         try {
            InternalCacheRegistry icr = TestingUtil.extractGlobalComponent(cm, InternalCacheRegistry.class);
            icr.registerInternalCache("__internal_test__", internalCacheConfig.build(),
                  EnumSet.of(InternalCacheRegistry.Flag.EXCLUSIVE));
            cm.getCache("__internal_test__");
            long warnCount = StreamSupport.stream(logAppender.spliterator(), false)
                  .filter(s -> s.contains("ISPN000728")).count();
            assertEquals(0, warnCount);
         } finally {
            killCacheManagers(cm);
         }
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
