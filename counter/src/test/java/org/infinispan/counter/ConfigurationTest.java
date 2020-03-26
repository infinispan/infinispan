package org.infinispan.counter;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;
import static org.infinispan.test.TestingUtil.withCacheManager;

import java.nio.file.Paths;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.IllegalLifecycleStateException;
import org.infinispan.commons.test.Exceptions;
import org.infinispan.commons.util.Util;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.configuration.cache.Configuration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.AbstractCounterConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfiguration;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.configuration.Reliability;
import org.infinispan.counter.configuration.StrongCounterConfiguration;
import org.infinispan.counter.configuration.WeakCounterConfiguration;
import org.infinispan.counter.exception.CounterConfigurationException;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.partitionhandling.PartitionHandling;
import org.infinispan.test.AbstractCacheTest;
import org.infinispan.test.TestingUtil;
import org.infinispan.test.fwk.TestCacheManagerFactory;
import org.infinispan.transaction.TransactionMode;
import org.testng.AssertJUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Configuration test
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "unit", testName = "counter.ConfigurationTest")
public class ConfigurationTest extends AbstractCacheTest {

   private static final String PERSISTENT_FOLDER = tmpDirectory(ConfigurationTest.class.getSimpleName());
   private static final String TEMP_PERSISTENT_FOLDER = Paths.get(PERSISTENT_FOLDER, "temp").toString();
   private static final String SHARED_PERSISTENT_FOLDER = Paths.get(PERSISTENT_FOLDER, "shared").toString();

   private static GlobalConfigurationBuilder defaultGlobalConfigurationBuilder(boolean globalStateEnabled) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.globalState().enabled(globalStateEnabled).persistentLocation(PERSISTENT_FOLDER)
            .temporaryLocation(TEMP_PERSISTENT_FOLDER)
            .sharedPersistentLocation(SHARED_PERSISTENT_FOLDER);
      return builder;
   }

   private static void assertCounterAndCacheConfiguration(CounterManagerConfiguration config,
         Configuration cacheConfig) {
      AssertJUnit.assertEquals(CacheMode.DIST_SYNC, cacheConfig.clustering().cacheMode());
      AssertJUnit.assertEquals(config.numOwners(), cacheConfig.clustering().hash().numOwners());
      AssertJUnit.assertEquals(config.reliability() == Reliability.CONSISTENT,
            cacheConfig.clustering().partitionHandling().whenSplit() == PartitionHandling.DENY_READ_WRITES);
      AssertJUnit.assertFalse(cacheConfig.clustering().l1().enabled());
      AssertJUnit.assertEquals(TransactionMode.NON_TRANSACTIONAL, cacheConfig.transaction().transactionMode());
   }

   @AfterMethod(alwaysRun = true)
   public void removeFiles() {
      Util.recursiveFileRemove(PERSISTENT_FOLDER);
      Util.recursiveFileRemove(TEMP_PERSISTENT_FOLDER);
      Util.recursiveFileRemove(SHARED_PERSISTENT_FOLDER);
   }

   private static Configuration getCounterCacheConfiguration(EmbeddedCacheManager cacheManager) {
      return cacheManager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME).getCacheConfiguration();
   }

   private static EmbeddedCacheManager buildCacheManager(GlobalConfigurationBuilder builder) {
      DefaultCacheManager cacheManager = new DefaultCacheManager(builder.build());
      //result doesn't matter. isDefined will wait until the caches are started to avoid starting and killing
      //caches too fast
      asCounterManager(cacheManager).isDefined("some-counter");
      return cacheManager;
   }

   public void testDefaultConfiguration() {
      TestingUtil.withCacheManager(() -> buildCacheManager(defaultGlobalConfigurationBuilder(false)),
            cacheManager -> {
               CounterManagerConfiguration configuration = CounterManagerConfigurationBuilder.defaultConfiguration();
               Configuration cacheConfiguration = getCounterCacheConfiguration(cacheManager);
               assertCounterAndCacheConfiguration(configuration, cacheConfiguration);
            });
   }

   public void testNumOwner() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      final CounterManagerConfiguration config = builder.addModule(CounterManagerConfigurationBuilder.class)
            .numOwner(5).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getCounterCacheConfiguration(cacheManager);
         assertCounterAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testReliability() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      final CounterManagerConfiguration config = builder.addModule(CounterManagerConfigurationBuilder.class)
            .reliability(Reliability.AVAILABLE).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getCounterCacheConfiguration(cacheManager);
         assertCounterAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testReliability2() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      final CounterManagerConfiguration config = builder.addModule(CounterManagerConfigurationBuilder.class)
            .reliability(Reliability.CONSISTENT).create();
      TestingUtil.withCacheManager(() -> buildCacheManager(builder), cacheManager -> {
         Configuration cacheConfiguration = getCounterCacheConfiguration(cacheManager);
         assertCounterAndCacheConfiguration(config, cacheConfiguration);
      });
   }

   public void testInvalidReliability() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);

      counterBuilder.reliability(Reliability.AVAILABLE);
      builder.build();

      counterBuilder.reliability(Reliability.CONSISTENT);
      builder.build();

      counterBuilder.reliability(null);
      assertCounterConfigurationException(builder);
   }

   public void testInvalidNumOwner() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);

      counterBuilder.numOwner(0);
      assertCounterConfigurationException(builder);

      counterBuilder.numOwner(-1);
      assertCounterConfigurationException(builder);

      counterBuilder.numOwner(1);
      builder.build();
   }


   public void testDuplicateCounterName() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addStrongCounter().name("aCounter");
      counterBuilder.addWeakCounter().name("aCounter");
      assertCounterConfigurationException(builder);

      counterBuilder.clearCounters();
      counterBuilder.addStrongCounter().name("aCounter");
      counterBuilder.addStrongCounter().name("aCounter");
      assertCounterConfigurationException(builder);

      counterBuilder.clearCounters();
      counterBuilder.addWeakCounter().name("aCounter");
      counterBuilder.addWeakCounter().name("aCounter");
      assertCounterConfigurationException(builder);
   }

   public void testMissingCounterName() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addStrongCounter();
      assertCounterConfigurationException(builder);

      counterBuilder.clearCounters();
      counterBuilder.addWeakCounter();
      assertCounterConfigurationException(builder);
   }

   public void testStrongCounterUpperBound() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addStrongCounter().name("valid").initialValue(10).upperBound(10);
      builder.build(); //no exception!

      counterBuilder.clearCounters();
      counterBuilder.addStrongCounter().name("valid").initialValue(10).upperBound(11);
      builder.build();

      counterBuilder.clearCounters();
      counterBuilder.addStrongCounter().name("invalid").initialValue(10).upperBound(9);
      assertCounterConfigurationException(builder);
   }

   public void testStringCounterLowerBound() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addStrongCounter().name("valid").initialValue(10).lowerBound(10);
      builder.build();

      counterBuilder.clearCounters();
      counterBuilder.addStrongCounter().name("valid").initialValue(10).lowerBound(9);
      builder.build();

      counterBuilder.clearCounters();
      counterBuilder.addStrongCounter().name("invalid").initialValue(10).lowerBound(11);
      assertCounterConfigurationException(builder);
   }

   public void testInvalidWeakCounterConcurrencyLevel() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addWeakCounter().name("invalid").concurrencyLevel(0);
      assertCounterConfigurationException(builder);

      counterBuilder.clearCounters();
      counterBuilder.addWeakCounter().name("invalid").concurrencyLevel(-1);
      assertCounterConfigurationException(builder);

      counterBuilder.clearCounters();
      counterBuilder.addWeakCounter().name("valid").concurrencyLevel(1);
      builder.build();
   }

   public void testInvalidStorage() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(true);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addWeakCounter().name("valid").storage(Storage.VOLATILE);
      counterBuilder.addStrongCounter().name("valid2").storage(Storage.PERSISTENT);
      builder.build();

      counterBuilder.clearCounters();
      counterBuilder.addWeakCounter().name("valid").storage(Storage.PERSISTENT);
      counterBuilder.addStrongCounter().name("valid2").storage(Storage.VOLATILE);
      builder.build();

      counterBuilder.clearCounters();
      counterBuilder.addWeakCounter().name("invalid").storage(null);
      assertCounterConfigurationException(builder);

      counterBuilder.clearCounters();
      counterBuilder.addStrongCounter().name("invalid").storage(null);
      assertCounterConfigurationException(builder);
   }

   public void testCounters() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(true);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addStrongCounter().name("unbounded-strong-1").initialValue(1).storage(Storage.VOLATILE)
            .addStrongCounter().name("lower-bounded-strong-2").initialValue(2).lowerBound(-10)
            .storage(Storage.PERSISTENT)
            .addStrongCounter().name("upper-bounded-strong-3").initialValue(3).upperBound(10)
            .storage(Storage.VOLATILE)
            .addStrongCounter().name("bounded-strong-4").initialValue(4).lowerBound(-20).upperBound(20)
            .storage(Storage.PERSISTENT)
            .addWeakCounter().name("weak-5").initialValue(5).concurrencyLevel(10).storage(Storage.VOLATILE);
      GlobalConfiguration config = builder.build();
      CounterManagerConfiguration counterConfig = config.module(CounterManagerConfiguration.class);
      assertUnboundedStrongCounter(counterConfig);
      assertBoundedStrongCounter(counterConfig, "lower-bounded-strong-2", 2, -10, Long.MAX_VALUE,
            Storage.PERSISTENT);
      assertBoundedStrongCounter(counterConfig, "upper-bounded-strong-3", 3, Long.MIN_VALUE, 10,
            Storage.VOLATILE);
      assertBoundedStrongCounter(counterConfig, "bounded-strong-4", 4, -20, 20, Storage.PERSISTENT);
      assertWeakCounter(counterConfig);

      TestingUtil.withCacheManager(() -> new DefaultCacheManager(builder.build()), cacheManager -> {
         CounterManager manager = asCounterManager(cacheManager);
         AssertJUnit.assertTrue(manager.isDefined("unbounded-strong-1"));
         AssertJUnit.assertTrue(manager.isDefined("lower-bounded-strong-2"));
         AssertJUnit.assertTrue(manager.isDefined("upper-bounded-strong-3"));
         AssertJUnit.assertTrue(manager.isDefined("bounded-strong-4"));
         AssertJUnit.assertTrue(manager.isDefined("weak-5"));
         AssertJUnit.assertFalse(manager.isDefined("not-defined-counter"));
      });
   }

   public void testInvalidEqualsUpperAndLowerBound() {
      final GlobalConfigurationBuilder builder = defaultGlobalConfigurationBuilder(false);
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      counterBuilder.addStrongCounter().name("invalid").initialValue(10).lowerBound(10).upperBound(10);
      assertCounterConfigurationException(builder);
   }

   public void testInvalidEqualsUpperAndLowerBoundInManager() {
      TestingUtil.withCacheManager(DefaultCacheManager::new, cacheManager -> {
         CounterManager manager = asCounterManager(cacheManager);
         CounterConfiguration cfg = CounterConfiguration.builder(CounterType.BOUNDED_STRONG).initialValue(10)
               .lowerBound(10).upperBound(10).build();
         Exceptions.expectException(CounterConfigurationException.class, () -> manager.defineCounter("invalid", cfg));
      });
   }

   private void assertUnboundedStrongCounter(CounterManagerConfiguration config) {
      for (AbstractCounterConfiguration counterConfig : config.counters()) {
         if (counterConfig.name().equals("unbounded-strong-1")) {
            AssertJUnit.assertTrue(counterConfig instanceof StrongCounterConfiguration);
            AssertJUnit.assertEquals(1, counterConfig.initialValue());
            AssertJUnit.assertEquals(Storage.VOLATILE, counterConfig.storage());
            return;
         }
      }
      AssertJUnit.fail();
   }

   private void assertWeakCounter(CounterManagerConfiguration config) {
      for (AbstractCounterConfiguration counterConfig : config.counters()) {
         if (counterConfig.name().equals("weak-5")) {
            AssertJUnit.assertTrue(counterConfig instanceof WeakCounterConfiguration);
            AssertJUnit.assertEquals(5, counterConfig.initialValue());
            AssertJUnit.assertEquals(Storage.VOLATILE, counterConfig.storage());
            AssertJUnit.assertEquals(10, ((WeakCounterConfiguration) counterConfig).concurrencyLevel());
            return;
         }
      }
      AssertJUnit.fail();
   }

   private void assertBoundedStrongCounter(CounterManagerConfiguration config, String name, long initialValue, long min,
         long max, Storage storage) {
      for (AbstractCounterConfiguration counterConfig : config.counters()) {
         if (counterConfig.name().equals(name)) {
            AssertJUnit.assertTrue(counterConfig instanceof StrongCounterConfiguration);
            AssertJUnit.assertEquals(initialValue, counterConfig.initialValue());
            AssertJUnit.assertEquals(storage, counterConfig.storage());
            AssertJUnit.assertTrue(((StrongCounterConfiguration) counterConfig).isBound());
            AssertJUnit.assertEquals(min, ((StrongCounterConfiguration) counterConfig).lowerBound());
            AssertJUnit.assertEquals(max, ((StrongCounterConfiguration) counterConfig).upperBound());
            return;
         }
      }
      AssertJUnit.fail();
   }

   private void assertCounterConfigurationException(GlobalConfigurationBuilder builder) {
      try {
         builder.build();
         AssertJUnit.fail("CacheConfigurationExpected");
      } catch (CounterConfigurationException | CacheConfigurationException expected) {
         log.trace("Expected", expected);
      }
   }

   public void testLocalManagerNotStarted() {
      withCacheManager(TestCacheManagerFactory.createCacheManager(false), cm -> {
         Exceptions.expectException(IllegalLifecycleStateException.class, () -> EmbeddedCounterManagerFactory.asCounterManager(cm));
      });
   }
}
