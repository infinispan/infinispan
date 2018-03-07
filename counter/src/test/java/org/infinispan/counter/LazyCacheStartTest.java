package org.infinispan.counter;

import static org.testng.AssertJUnit.assertFalse;

import java.util.concurrent.TimeUnit;

import org.infinispan.counter.api.CounterConfiguration;
import org.infinispan.counter.api.CounterType;
import org.infinispan.counter.impl.BaseCounterTest;
import org.infinispan.counter.impl.CounterModuleLifecycle;
import org.infinispan.manager.EmbeddedCacheManager;
import org.testng.annotations.Test;

/**
 * Tests the lazy cache creation
 *
 * @author Pedro Ruivo
 * @since 9.2
 */
@Test(groups = "functional", testName = "counter.LazyCacheStartTest")
public class LazyCacheStartTest extends BaseCounterTest {
   public void testLazyStart() {
      for (EmbeddedCacheManager manager : cacheManagers) {
         assertFalse(manager.isRunning(CounterModuleLifecycle.COUNTER_CACHE_NAME));
      }
      counterManager(0).defineCounter("some-counter", CounterConfiguration.builder(CounterType.WEAK).build());
      for (EmbeddedCacheManager manager : cacheManagers) {
         eventually(() -> "counter cache didn't start in " + manager.getTransport().getAddress(),
               () -> manager.isRunning(CounterModuleLifecycle.COUNTER_CACHE_NAME),
               30, TimeUnit.SECONDS);
      }
   }

   @Override
   protected int clusterSize() {
      return 3;
   }

}
