package org.infinispan.server.hotrod.counter.impl;

import static java.lang.Math.abs;
import static org.infinispan.counter.api.CounterConfiguration.builder;
import static org.infinispan.counter.impl.Util.awaitCounterOperation;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;

import org.infinispan.Cache;
import org.infinispan.counter.EmbeddedCounterManagerFactory;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.Storage;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.manager.CounterConfigurationManager;
import org.infinispan.globalstate.GlobalConfigurationManager;
import org.infinispan.globalstate.ScopedState;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.server.hotrod.counter.CounterManagerTestStrategy;
import org.infinispan.util.logging.Log;

/**
 * The {@link CounterManagerTestStrategy} implementation.
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
public class CounterManagerImplTestStrategy implements CounterManagerTestStrategy {

   private final Supplier<List<CounterManager>> allRemoteCounterManagerSupplier;
   private final Supplier<Log> logSupplier;
   private final Supplier<EmbeddedCacheManager> cacheManagerSupplier;

   public CounterManagerImplTestStrategy(Supplier<List<CounterManager>> allRemoteCounterManagerSupplier,
         Supplier<Log> logSupplier, Supplier<EmbeddedCacheManager> cacheManagerSupplier) {
      this.allRemoteCounterManagerSupplier = allRemoteCounterManagerSupplier;
      this.logSupplier = logSupplier;
      this.cacheManagerSupplier = cacheManagerSupplier;
   }

   @Override
   public void testWeakCounter(Method method) {
      final Random random = generateRandom();
      final String counterName = method.getName();
      CounterConfiguration config = builder(CounterType.WEAK)
            .initialValue(random.nextInt())
            .storage(random.nextBoolean() ? Storage.VOLATILE : Storage.PERSISTENT)
            .concurrencyLevel(abs(random.nextInt()))
            .build();
      doCreationTest(counterName, config);
   }

   @Override
   public void testUnboundedStrongCounter(Method method) {
      final Random random = generateRandom();
      final String counterName = method.getName();
      CounterConfiguration config = builder(CounterType.UNBOUNDED_STRONG)
            .initialValue(random.nextInt())
            .storage(random.nextBoolean() ? Storage.VOLATILE : Storage.PERSISTENT)
            .build();
      doCreationTest(counterName, config);
   }

   @Override
   public void testUpperBoundedStrongCounter(Method method) {
      final Random random = generateRandom();
      final String counterName = method.getName();
      CounterConfiguration config = builder(CounterType.BOUNDED_STRONG)
            .initialValue(5)
            .upperBound(15)
            .storage(random.nextBoolean() ? Storage.VOLATILE : Storage.PERSISTENT)
            .build();
      doCreationTest(counterName, config);
   }

   @Override
   public void testLowerBoundedStrongCounter(Method method) {
      final Random random = generateRandom();
      final String counterName = method.getName();
      CounterConfiguration config = builder(CounterType.BOUNDED_STRONG)
            .initialValue(15)
            .lowerBound(5)
            .storage(random.nextBoolean() ? Storage.VOLATILE : Storage.PERSISTENT)
            .build();
      doCreationTest(counterName, config);
   }

   @Override
   public void testBoundedStrongCounter(Method method) {
      final Random random = generateRandom();
      final String counterName = method.getName();
      CounterConfiguration config = builder(CounterType.BOUNDED_STRONG)
            .initialValue(15)
            .lowerBound(5)
            .upperBound(20)
            .storage(random.nextBoolean() ? Storage.VOLATILE : Storage.PERSISTENT)
            .build();
      doCreationTest(counterName, config);
   }

   @Override
   public void testUndefinedCounter() {
      CounterManager counterManager = getTestedCounterManager();
      assertFalse(counterManager.isDefined("not-defined-counter"));
      assertEquals(null, counterManager.getConfiguration("not-defined-counter"));
   }

   @Override
   public void testRemove(Method method) {
      //we need to cleanup other tests counters from the caches because of cache.size()
      clearCaches();

      final Random random = generateRandom();
      final String counterName = method.getName();
      final CounterManager counterManager = getTestedCounterManager();
      CounterConfiguration config = builder(CounterType.UNBOUNDED_STRONG).initialValue(random.nextLong()).build();
      assertTrue(counterManager.defineCounter(counterName, config));
      awaitCounterOperation(counterManager.getStrongCounter(counterName).addAndGet(10));
      EmbeddedCacheManager cacheManager = cacheManagerSupplier.get();
      Cache<?, ?> cache = cacheManager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME);
      assertEquals(1, cache.size());
      counterManager.remove(counterName);
      assertEquals(0, cache.size());
   }

   @Override
   public void testGetCounterNames(Method method) {
      //we need to cleanup other tests counters from the caches.
      clearCaches();

      final Random random = generateRandom();
      final String counterNamePrefix = method.getName();
      final CounterManager counterManager = getTestedCounterManager();

      final int numCounters = random.nextInt(10) + 1;
      final List<CounterConfiguration> configList = new ArrayList<>(numCounters);
      final Set<String> counterSet = new HashSet<>();

      //adds some randomness to the test by adding 1 to 10 counters
      for (int i = 0; i < numCounters; ++i) {
         CounterConfiguration config = builder(CounterType.valueOf(random.nextInt(3))).initialValue(random.nextLong())
               .build();
         assertTrue(counterManager.defineCounter(counterNamePrefix + i, config));
         configList.add(config);
         counterSet.add(counterNamePrefix + i);
      }

      Set<String> counterNames = new HashSet<>(counterManager.getCounterNames());
      assertEquals(counterSet, counterNames);

      for (int i = 0; i < numCounters; ++i) {
         final String counterName = counterNamePrefix + i;
         assertTrue(counterNames.contains(counterName));
         CounterConfiguration config = configList.get(i);
         CounterConfiguration storedConfig = config.type() == CounterType.WEAK ?
                                             counterManager.getWeakCounter(counterName).getConfiguration() :
                                             counterManager.getStrongCounter(counterName).getConfiguration();
         assertEquals(config, storedConfig);
      }

   }

   private Random generateRandom() {
      final long seed = System.nanoTime();
      logSupplier.get().debugf("Using SEED: '%d'", seed);
      return new Random(seed);
   }

   private void doCreationTest(String name, CounterConfiguration config) {
      List<CounterManager> remoteCounterManagers = allRemoteCounterManagerSupplier.get();
      assertTrue(remoteCounterManagers.get(0).defineCounter(name, config));
      remoteCounterManagers.forEach(cm -> assertFalse(cm.defineCounter(name, builder(CounterType.WEAK).build())));
      remoteCounterManagers.forEach(cm -> assertTrue(cm.isDefined(name)));
      remoteCounterManagers.forEach(cm -> assertEquals(config, cm.getConfiguration(name)));

      //test single embedded counter manager to check if everything is correctly stored
      EmbeddedCacheManager cacheManager = cacheManagerSupplier.get();
      CounterManager counterManager = EmbeddedCounterManagerFactory.asCounterManager(cacheManager);
      assertTrue(counterManager.isDefined(name));
      assertEquals(config, counterManager.getConfiguration(name));
   }

   private CounterManager getTestedCounterManager() {
      return allRemoteCounterManagerSupplier.get().get(0);
   }

   private void clearCaches() {
      //we need to cleanup other tests counter from the caches.
      EmbeddedCacheManager cacheManager = cacheManagerSupplier.get();
      cacheManager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME).clear();
      cacheManager.<ScopedState, Object>getCache(GlobalConfigurationManager.CONFIG_STATE_CACHE_NAME).keySet().removeIf(o -> CounterConfigurationManager.COUNTER_SCOPE.equals(o.getScope()));
   }

}
