package org.infinispan.counter;

import static org.infinispan.counter.EmbeddedCounterManagerFactory.asCounterManager;

import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.configuration.cache.CacheMode;
import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterManager;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.api.WeakCounter;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.counter.impl.weak.WeakCounterImpl;
import org.infinispan.counter.impl.weak.WeakCounterKey;
import org.infinispan.distribution.DistributionManager;
import org.infinispan.distribution.LocalizedCacheTopology;
import org.infinispan.manager.EmbeddedCacheManager;
import org.infinispan.test.fwk.CleanupAfterMethod;
import org.testng.AssertJUnit;
import org.testng.annotations.Test;

/**
 * Tests the key distribution for {@link WeakCounter}.
 *
 * @author Pedro Ruivo
 * @since 9.0
 */
@Test(groups = "functional", testName = "counter.WeakCounterKeyDistributionTest")
@CleanupAfterMethod
public class WeakCounterKeyDistributionTest extends BaseCounterTest {

   private static final int CLUSTER_SIZE = 4;

   private static void assertKeyDistribution(Cache<?, ?> cache, String counterName) {
      WeakCounterImpl counter = getCounter(cache.getCacheManager(), counterName);
      DistributionManager distributionManager = cache.getAdvancedCache().getDistributionManager();
      LocalizedCacheTopology topology = distributionManager.getCacheTopology();

      Set<WeakCounterKey> preferredKeys = new HashSet<>();
      WeakCounterKey[] keys = counter.getPreferredKeys();
      if (keys != null) {
         for (WeakCounterKey key : keys) {
            AssertJUnit.assertTrue(topology.getDistribution(key).isPrimary());
            AssertJUnit.assertTrue(preferredKeys.add(key));
         }
      }

      for (WeakCounterKey key : counter.getKeys()) {
         if (!preferredKeys.remove(key)) {
            AssertJUnit.assertFalse(topology.getDistribution(key).isPrimary());
         }
      }
      AssertJUnit.assertTrue(preferredKeys.isEmpty());
   }

   private static WeakCounterImpl getCounter(EmbeddedCacheManager manager, String counterName) {
      CounterManager counterManager = asCounterManager(manager);
      counterManager
            .defineCounter(counterName, CounterConfiguration.builder(CounterType.WEAK).concurrencyLevel(128).build());
      return (WeakCounterImpl) counterManager.getWeakCounter(counterName);
   }

   public void testKeyDistribution(Method method) {
      final String counterName = method.getName();

      assertKeyDistributionInAllManagers(counterName);
   }

   public void testKeyDistributionAfterJoin(Method method) {
      final String counterName = method.getName();
      assertKeyDistributionInAllManagers(counterName);

      addClusterEnabledCacheManager(configure(cacheManagers.size()), getDefaultClusteredCacheConfig(CacheMode.DIST_SYNC));
      waitForCounterCaches();

      assertKeyDistributionInAllManagers(counterName);
   }

   public void testKeyDistributionAfterLeave(Method method) {
      final String counterName = method.getName();
      assertKeyDistributionInAllManagers(counterName);

      killMember(1);
      waitForCounterCaches();

      assertKeyDistributionInAllManagers(counterName);
   }

   @Override
   protected int clusterSize() {
      return CLUSTER_SIZE;
   }

   private void assertKeyDistributionInAllManagers(String counterName) {
      for (EmbeddedCacheManager manager : getCacheManagers()) {
         assertKeyDistribution(manager.getCache(CounterModuleLifecycle.COUNTER_CACHE_NAME), counterName);
      }
   }
}
