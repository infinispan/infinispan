package org.infinispan.counter;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;

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
@Test(groups = "functional", testName = "counter.EagerCacheStartTest")
public class EagerCacheStartTest extends BaseCounterTest {
   public void testLazyStart() {
      for (EmbeddedCacheManager manager : cacheManagers) {
         assertTrue(manager.isRunning(CounterModuleLifecycle.COUNTER_CACHE_NAME));
      }
      counterManager(0).defineCounter("some-counter", CounterConfiguration.builder(CounterType.WEAK).build());
      assertEquals(0, counterManager(0).getWeakCounter("some-counter").getValue());
   }

   @Override
   protected int clusterSize() {
      return 3;
   }

}
