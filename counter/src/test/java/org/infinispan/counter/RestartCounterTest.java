package org.infinispan.counter;

import static org.infinispan.commons.test.CommonsTestingUtil.tmpDirectory;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Collectors;

import org.infinispan.commons.util.Util;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.configuration.CounterManagerConfigurationBuilder;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.RestartCounterTest")
@CleanupAfterMethod
public class RestartCounterTest extends BaseCounterTest {

   private static final String PERSISTENT_FOLDER = tmpDirectory(RestartCounterTest.class.getSimpleName());
   private static final String TEMP_PERSISTENT_FOLDER = Paths.get(PERSISTENT_FOLDER, "temp").toString();
   private static final String SHARED_PERSISTENT_FOLDER = Paths.get(PERSISTENT_FOLDER, "shared").toString();
   private static final int CLUSTER_SIZE = 4;
   private final Collection<CounterDefinition> defaultCounters = new ArrayList<>(6);
   private final Collection<CounterDefinition> otherCounters = new ArrayList<>(6);

   public RestartCounterTest() {
      defaultCounters.add(new CounterDefinition("v-bounded", CounterType.BOUNDED_STRONG, Storage.VOLATILE));
      defaultCounters.add(new CounterDefinition("p-bounded", CounterType.BOUNDED_STRONG, Storage.PERSISTENT));
      defaultCounters.add(new CounterDefinition("v-unbounded", CounterType.UNBOUNDED_STRONG, Storage.VOLATILE));
      defaultCounters.add(new CounterDefinition("p-unbounded", CounterType.UNBOUNDED_STRONG, Storage.PERSISTENT));
      defaultCounters.add(new CounterDefinition("v-weak", CounterType.WEAK, Storage.VOLATILE));
      defaultCounters.add(new CounterDefinition("p-weak", CounterType.WEAK, Storage.PERSISTENT));

      otherCounters.add(new CounterDefinition("o-v-bounded", CounterType.BOUNDED_STRONG, Storage.VOLATILE));
      otherCounters.add(new CounterDefinition("o-p-bounded", CounterType.BOUNDED_STRONG, Storage.PERSISTENT));
      otherCounters.add(new CounterDefinition("o-v-unbounded", CounterType.UNBOUNDED_STRONG, Storage.VOLATILE));
      otherCounters.add(new CounterDefinition("o-p-unbounded", CounterType.UNBOUNDED_STRONG, Storage.PERSISTENT));
      otherCounters.add(new CounterDefinition("o-v-weak", CounterType.WEAK, Storage.VOLATILE));
      otherCounters.add(new CounterDefinition("o-p-weak", CounterType.WEAK, Storage.PERSISTENT));
   }

   private static void incrementAll(Collection<CounterDefinition> counters, CounterManager counterManager) {
      counters.forEach(counterDefinition -> counterDefinition.incrementCounter(counterManager));
   }

   @AfterMethod(alwaysRun = true)
   public void removeFiles() {
      Util.recursiveFileRemove(PERSISTENT_FOLDER);
   }

   public void testCountersInConfiguration() {
      assertDefined(defaultCounters);

      //while the retries during state transfer aren't fixed, we need to wait for the cache to start everywhere
      waitForClusterToForm(CounterModuleLifecycle.COUNTER_CACHE_NAME);

      incrementAll(defaultCounters, counterManager(0));
      assertCounterValue(defaultCounters, counterManager(0), 1, 1);
      shutdownAndRestart();
      assertDefined(defaultCounters);
      assertCounterValue(defaultCounters, counterManager(0), 0, 1);

      incrementAll(defaultCounters, counterManager(0));

      assertCounterValue(defaultCounters, counterManager(0), 1, 2);
   }

   public void testRuntimeCounters() {
      final CounterManager counterManager = counterManager(0);

      //while the retries during state transfer aren't fixed, we need to wait for the cache to start everywhere
      waitForClusterToForm(CounterModuleLifecycle.COUNTER_CACHE_NAME);

      incrementAll(defaultCounters, counterManager);
      incrementAll(defaultCounters, counterManager);
      otherCounters.forEach(counterDefinition -> counterDefinition.define(counterManager));
      incrementAll(otherCounters, counterManager);

      assertCounterValue(defaultCounters, counterManager, 2, 2);
      assertCounterValue(otherCounters, counterManager, 1, 1);

      shutdownAndRestart();

      //recreate the counter manager.
      final CounterManager counterManager2 = counterManager(0);
      Collection<CounterDefinition> othersPersisted = otherCounters.stream()
            .filter(counterDefinition -> counterDefinition.storage == Storage.PERSISTENT).collect(
                  Collectors.toList());
      Collection<CounterDefinition> otherVolatile = otherCounters.stream()
            .filter(counterDefinition -> counterDefinition.storage == Storage.VOLATILE).collect(
                  Collectors.toList());
      assertDefined(defaultCounters);
      assertDefined(othersPersisted);
      assertNotDefined(otherVolatile);
      assertCounterValue(defaultCounters, counterManager2, 0, 2);
      assertCounterValue(othersPersisted, counterManager2, -1 /*doesn't mather*/, 1);

      incrementAll(defaultCounters, counterManager2);
      incrementAll(othersPersisted, counterManager2);

      assertCounterValue(defaultCounters, counterManager2, 1, 3);
      assertCounterValue(othersPersisted, counterManager2, -1 /*doesn't mather*/, 2);
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   @Override
   protected GlobalConfigurationBuilder configure(int nodeId) {
      GlobalConfigurationBuilder builder = GlobalConfigurationBuilder.defaultClusteredBuilder();
      builder.globalState().enable().persistentLocation(Paths.get(PERSISTENT_FOLDER, Integer.toString(nodeId)).toString())
            .temporaryLocation(Paths.get(TEMP_PERSISTENT_FOLDER, Integer.toString(nodeId)).toString())
            .sharedPersistentLocation(Paths.get(SHARED_PERSISTENT_FOLDER, Integer.toString(nodeId)).toString());
      CounterManagerConfigurationBuilder counterBuilder = builder.addModule(CounterManagerConfigurationBuilder.class);
      defaultCounters.forEach(counterDefinition -> counterDefinition.define(counterBuilder));
      return builder;
   }

   private void shutdownAndRestart() {
      cache(0, CounterModuleLifecycle.COUNTER_CACHE_NAME).shutdown();
      log.debug("Shutdown caches");
      cacheManagers.forEach(EmbeddedCacheManager::stop);
      cacheManagers.clear();

      log.debug("Restart caches");
      createCacheManagers();
   }

   private void assertDefined(Collection<CounterDefinition> counters) {
      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         CounterManager counterManager = counterManager(i);
         for (CounterDefinition definition : counters) {
            assertTrue("Configuration of " + definition.name + " is missing on manager " + i,
                  counterManager.isDefined(definition.name));
         }
      }
   }

   private void assertNotDefined(Collection<CounterDefinition> counters) {
      for (int i = 0; i < CLUSTER_SIZE; ++i) {
         CounterManager counterManager = counterManager(i);
         for (CounterDefinition definition : counters) {
            assertFalse("Configuration of " + definition.name + " is defined on manager " + i,
                  counterManager.isDefined(definition.name));
         }
      }
   }

   private void assertCounterValue(Collection<CounterDefinition> counters, CounterManager counterManager,
         long volatileValue, long persistentValue) {
      for (CounterDefinition definition : counters) {
         long expect = definition.storage == Storage.VOLATILE ? volatileValue : persistentValue;
         eventuallyEquals("Wrong value for counter " + definition.name, expect,
               () -> definition.getValue(counterManager));
      }
   }

   private static class CounterDefinition {
      private final String name;
      private final CounterType type;
      private final Storage storage;

      private CounterDefinition(String name, CounterType type, Storage storage) {
         this.name = name;
         this.type = type;
         this.storage = storage;
      }

      private void define(CounterManagerConfigurationBuilder builder) {
         switch (type) {
            case UNBOUNDED_STRONG:
               builder.addStrongCounter().name(name).storage(storage);
               break;
            case BOUNDED_STRONG:
               builder.addStrongCounter().name(name).lowerBound(0).storage(storage);
               break;
            case WEAK:
               builder.addWeakCounter().name(name).storage(storage).concurrencyLevel(16);
         }
      }

      private void define(CounterManager manager) {
         //lower bound is ignored if the type is not bounded.
         manager.defineCounter(name, CounterConfiguration.builder(type).lowerBound(0).storage(storage).build());
      }

      private void incrementCounter(CounterManager counterManager) {
         switch (type) {
            case UNBOUNDED_STRONG:
            case BOUNDED_STRONG:
               counterManager.getStrongCounter(name).sync().incrementAndGet();
               break;
            case WEAK:
               counterManager.getWeakCounter(name).sync().increment();
               break;
         }
      }

      private long getValue(CounterManager counterManager) {
         switch (type) {
            case WEAK:
               return counterManager.getWeakCounter(name).getValue();
            case BOUNDED_STRONG:
            case UNBOUNDED_STRONG:
               return counterManager.getStrongCounter(name).sync().getValue();
         }
         throw new IllegalStateException();
      }
   }


}
