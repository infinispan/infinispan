package org.infinispan.container.impl;

import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertTrue;
import static org.testng.AssertJUnit.fail;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import org.infinispan.commons.CacheConfigurationException;
import org.infinispan.commons.util.MemoryMonitor;
import org.infinispan.configuration.global.ContainerMemoryConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.configuration.global.GlobalConfigurationBuilder;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

@Test(groups = "functional", testName = "container.DynamicMemoryResizerIntegrationTest")
public class DynamicMemoryResizerIntegrationTest extends AbstractInfinispanTest {

   private MemoryMonitor monitor;
   private ScheduledExecutorService executor;
   private DynamicMemoryResizer resizer;

   @AfterMethod
   public void cleanup() {
      if (resizer != null) {
         resizer.stop();
      }
      if (monitor != null) {
         monitor.stop();
      }
      if (executor != null) {
         executor.shutdownNow();
      }
   }

   @Test
   public void testShrinkWithRealCaffeine() {
      SharedCaffeineMap<String, String> sharedMap = new SharedCaffeineMap<>(1000, false);
      assertEquals(1000, sharedMap.capacity());

      monitor = new MemoryMonitor(0.85, 5000, 0.20, 10_000);
      executor = Executors.newSingleThreadScheduledExecutor();

      resizer = createResizer(sharedMap, "pool1", monitor, executor);

      // Trigger GC pressure
      monitor.recordGcEvent(100_000, 3000);
      assertTrue(monitor.isGcPressureExceeded());

      // Give executor time to dispatch callback
      eventually(() -> sharedMap.capacity() == 800, 5000);
      assertEquals(800, sharedMap.capacity());
   }

   @Test
   public void testGrowBackWithRealCaffeine() {
      SharedCaffeineMap<String, String> sharedMap = new SharedCaffeineMap<>(1000, false);
      monitor = new MemoryMonitor(0.85, 5000, 0.20, 10_000);
      executor = Executors.newSingleThreadScheduledExecutor();

      resizer = createResizer(sharedMap, "pool1", monitor, executor);

      // Trigger pressure
      monitor.recordGcEvent(100_000, 3000);
      eventually(() -> sharedMap.capacity() == 800, 5000);

      // Clear pressure
      monitor.recordGcEvent(120_000, 10);
      eventually(() -> !monitor.isGcPressureExceeded(), 5000);

      // The resizer should transition to GROWING once the async callback executes.
      // The grow delay is 30s by default, so manually invoke growStep to verify integration.
      eventually(() -> resizer.state == DynamicMemoryResizer.State.GROWING, 5000);
      resizer.growStep();
      assertEquals(900, sharedMap.capacity());
   }

   @Test
   public void testFloorWithRealCaffeine() {
      SharedCaffeineMap<String, String> sharedMap = new SharedCaffeineMap<>(100, false);
      monitor = new MemoryMonitor(0.85, 5000, 0.20, 10_000);
      executor = Executors.newSingleThreadScheduledExecutor();

      resizer = createResizer(sharedMap, "pool1", monitor, executor);

      // Trigger pressure multiple times
      Mockito.when(Mockito.mock(MemoryMonitor.class).isMemoryLow()).thenReturn(true);
      monitor.simulateMemoryLow();
      eventually(() -> sharedMap.capacity() < 100, 5000);

      // Keep shrinking
      long prevCapacity;
      do {
         prevCapacity = sharedMap.capacity();
         resizer.recheckShrink();
      } while (sharedMap.capacity() < prevCapacity);

      // Floor is 10% of 100 = 10
      assertTrue("Capacity should be at floor (10), was: " + sharedMap.capacity(),
            sharedMap.capacity() >= 10);
   }

   @Test
   public void testOnlyDynamicResizeContainersManaged() {
      SharedCaffeineMap<String, String> dynamicMap = new SharedCaffeineMap<>(1000, false);
      SharedCaffeineMap<String, String> staticMap = new SharedCaffeineMap<>(500, false);

      monitor = new MemoryMonitor(0.85, 5000, 0.20, 10_000);
      executor = Executors.newSingleThreadScheduledExecutor();

      ContainerMemoryConfiguration dynamicConfig = Mockito.mock(ContainerMemoryConfiguration.class);
      Mockito.when(dynamicConfig.dynamicResize()).thenReturn(true);
      ContainerMemoryConfiguration staticConfig = Mockito.mock(ContainerMemoryConfiguration.class);
      Mockito.when(staticConfig.dynamicResize()).thenReturn(false);

      GlobalConfiguration globalConfig = Mockito.mock(GlobalConfiguration.class);
      Mockito.when(globalConfig.getMemoryContainer()).thenReturn(Map.of(
            "dynamic", dynamicConfig,
            "static", staticConfig
      ));

      SharedContainerMaps sharedContainerMaps = Mockito.mock(SharedContainerMaps.class);
      //noinspection unchecked
      Mockito.when(sharedContainerMaps.getMaps()).thenReturn(Map.of(
            "dynamic", (SharedCaffeineMap<Object, Object>) (SharedCaffeineMap<?, ?>) dynamicMap,
            "static", (SharedCaffeineMap<Object, Object>) (SharedCaffeineMap<?, ?>) staticMap
      ));

      resizer = new DynamicMemoryResizer();
      TestingUtil.inject(resizer, globalConfig, sharedContainerMaps, monitor,
            new org.infinispan.factories.impl.TestComponentAccessors.NamedComponent(
                  org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, executor));
      resizer.start();

      // Trigger pressure
      monitor.recordGcEvent(100_000, 3000);
      eventually(() -> dynamicMap.capacity() == 800, 5000);
      assertEquals(500, staticMap.capacity());
   }

   @Test
   public void testConfigValidationFailsWithoutMemoryMonitor() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.cacheContainer().memoryMonitor().enabled(false);
      builder.containerMemoryConfiguration("pool1").maxCount(100).dynamicResize(true);

      try {
         builder.build();
         fail("Expected CacheConfigurationException");
      } catch (CacheConfigurationException e) {
         assertTrue(e.getMessage().contains("dynamic-resize"));
         assertTrue(e.getMessage().contains("memory-monitor"));
      }
   }

   @Test
   public void testConfigValidationSucceedsWithMemoryMonitor() {
      GlobalConfigurationBuilder builder = new GlobalConfigurationBuilder();
      builder.cacheContainer().memoryMonitor().enabled(true);
      builder.containerMemoryConfiguration("pool1").maxCount(100).dynamicResize(true);

      // Should not throw
      builder.build();
   }

   private DynamicMemoryResizer createResizer(SharedCaffeineMap<?, ?> map, String name,
         MemoryMonitor monitor, ScheduledExecutorService executor) {
      ContainerMemoryConfiguration containerConfig = Mockito.mock(ContainerMemoryConfiguration.class);
      Mockito.when(containerConfig.dynamicResize()).thenReturn(true);

      GlobalConfiguration globalConfig = Mockito.mock(GlobalConfiguration.class);
      Mockito.when(globalConfig.getMemoryContainer()).thenReturn(Map.of(name, containerConfig));

      SharedContainerMaps sharedContainerMaps = Mockito.mock(SharedContainerMaps.class);
      //noinspection unchecked
      Mockito.when(sharedContainerMaps.getMaps()).thenReturn(
            Map.of(name, (SharedCaffeineMap<Object, Object>) (SharedCaffeineMap<?, ?>) map));

      DynamicMemoryResizer resizer = new DynamicMemoryResizer();
      TestingUtil.inject(resizer, globalConfig, sharedContainerMaps, monitor,
            new org.infinispan.factories.impl.TestComponentAccessors.NamedComponent(
                  org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, executor));
      resizer.start();
      return resizer;
   }
}
