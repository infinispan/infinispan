package org.infinispan.container.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.infinispan.commons.util.MemoryMonitor;
import org.infinispan.configuration.global.ContainerMemoryConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.test.AbstractInfinispanTest;
import org.infinispan.test.TestingUtil;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test(groups = "unit", testName = "container.DynamicMemoryResizerTest")
public class DynamicMemoryResizerTest extends AbstractInfinispanTest {

   private DynamicMemoryResizer resizer;
   private MemoryMonitor memoryMonitor;
   private SharedCaffeineMap<Object, Object> sharedMap;
   private ScheduledExecutorService scheduledExecutor;
   private ScheduledFuture<?> mockFuture;
   private AtomicLong gcGen;

   private static final long ORIGINAL_CAPACITY = 1000;

   @SuppressWarnings("unchecked")
   @BeforeMethod
   public void setup() {
      resizer = new DynamicMemoryResizer();
      memoryMonitor = mock(MemoryMonitor.class);
      sharedMap = mock(SharedCaffeineMap.class);
      scheduledExecutor = mock(ScheduledExecutorService.class);
      mockFuture = mock(ScheduledFuture.class);

      gcGen = new AtomicLong();
      when(memoryMonitor.getGcGeneration()).thenAnswer(inv -> gcGen.get());

      when(sharedMap.capacity()).thenReturn(ORIGINAL_CAPACITY);
      doReturn(mockFuture).when(scheduledExecutor).schedule(any(Runnable.class), anyLong(), any(TimeUnit.class));

      ContainerMemoryConfiguration containerConfig = mock(ContainerMemoryConfiguration.class);
      when(containerConfig.dynamicResize()).thenReturn(true);

      GlobalConfiguration globalConfig = mock(GlobalConfiguration.class);
      when(globalConfig.getMemoryContainer()).thenReturn(Map.of("pool1", containerConfig));

      SharedContainerMaps sharedContainerMaps = mock(SharedContainerMaps.class);
      when(sharedContainerMaps.getMaps()).thenReturn(Map.of("pool1", sharedMap));

      TestingUtil.inject(resizer, globalConfig, sharedContainerMaps, memoryMonitor,
            new org.infinispan.factories.impl.TestComponentAccessors.NamedComponent(
                  org.infinispan.factories.KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR, scheduledExecutor));
      resizer.start();
   }

   @Test
   public void testShrinkOnMemoryLow() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.onMemoryLow();

      // 20% of 1000 = 200, so 1000 - 200 = 800
      verify(sharedMap).resize(800);
      assertEquals(DynamicMemoryResizer.State.SHRINKING, resizer.state);
   }

   @Test
   public void testShrinkOnGcPressure() {
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(true);

      resizer.onGcPressureHigh();

      verify(sharedMap).resize(800);
      assertEquals(DynamicMemoryResizer.State.SHRINKING, resizer.state);
   }

   @Test
   public void testRepeatedShrinking() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.onMemoryLow();
      verify(sharedMap).resize(800);

      // Simulate a subsequent GC completing while still under pressure
      gcGen.incrementAndGet();
      when(sharedMap.capacity()).thenReturn(800L);
      resizer.lastShrinkTimeMs = 0;
      resizer.onGcCompleted();

      // 800 - 200 = 600
      verify(sharedMap).resize(600);
   }

   @Test
   public void testFloorEnforcement() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      // Shrink to near floor: capacity at 150, floor is 100 (10% of 1000)
      when(sharedMap.capacity()).thenReturn(150L);
      resizer.transitionToShrinking();

      // 150 - 200 = -50 → clamped to floor 100
      verify(sharedMap).resize(100);
   }

   @Test
   public void testCannotShrinkBelowFloor() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      // Already at floor
      when(sharedMap.capacity()).thenReturn(100L);
      resizer.transitionToShrinking();

      // Should not resize since already at floor
      verify(sharedMap, never()).resize(anyLong());
   }

   @Test
   public void testGrowBackOnRecovery() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      // Shrink first
      resizer.onMemoryLow();
      verify(sharedMap).resize(800);

      // Now both signals clear
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.checkRecovery();
      assertEquals(DynamicMemoryResizer.State.GROWING, resizer.state);

      // Simulate grow step
      when(sharedMap.capacity()).thenReturn(800L);
      resizer.growStep();

      // 800 + 100 (10% of 1000) = 900
      verify(sharedMap).resize(900);
   }

   @Test
   public void testGrowBackToOriginal() {
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      // Set state to GROWING with capacity at 900
      resizer.state = DynamicMemoryResizer.State.GROWING;
      when(sharedMap.capacity()).thenReturn(900L);

      resizer.growStep();

      // 900 + 100 = 1000, capped at original
      verify(sharedMap).resize(1000);
      assertEquals(DynamicMemoryResizer.State.STABLE, resizer.state);
   }

   @Test
   public void testGrowBackExponentialBackoff() {
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.state = DynamicMemoryResizer.State.GROWING;
      resizer.currentGrowDelayMs = DynamicMemoryResizer.GROW_INITIAL_MS;

      // Capacity not fully recovered
      when(sharedMap.capacity()).thenReturn(600L);

      resizer.growStep();
      verify(sharedMap).resize(700);

      // Delay should have doubled
      assertEquals(DynamicMemoryResizer.GROW_INITIAL_MS * 2, resizer.currentGrowDelayMs);
   }

   @Test
   public void testGrowBackMaxBackoff() {
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.state = DynamicMemoryResizer.State.GROWING;
      resizer.currentGrowDelayMs = DynamicMemoryResizer.GROW_MAX_MS;

      when(sharedMap.capacity()).thenReturn(600L);
      resizer.growStep();

      // Should stay capped at max
      assertEquals(DynamicMemoryResizer.GROW_MAX_MS, resizer.currentGrowDelayMs);
   }

   @Test
   public void testGrowInterruptedByPressure() {
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.state = DynamicMemoryResizer.State.GROWING;
      when(sharedMap.capacity()).thenReturn(800L);

      // Pressure returns during grow step
      when(memoryMonitor.isMemoryLow()).thenReturn(true);

      resizer.growStep();

      assertEquals(DynamicMemoryResizer.State.SHRINKING, resizer.state);
      // Should have shrunk, not grown
      verify(sharedMap).resize(600);
   }

   @Test
   public void testSingleSignalClearStaysShrinking() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(true);

      resizer.onMemoryLow();
      assertEquals(DynamicMemoryResizer.State.SHRINKING, resizer.state);

      // Only memory recovered, GC pressure still high
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(true);

      resizer.checkRecovery();
      // Should stay SHRINKING because GC pressure is still active
      assertEquals(DynamicMemoryResizer.State.SHRINKING, resizer.state);
   }

   @Test
   public void testRecheckTransitionsToGrowingWhenPressureClears() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      resizer.onMemoryLow();

      // A subsequent GC completes and pressure clears
      gcGen.incrementAndGet();
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.lastShrinkTimeMs = 0;
      resizer.onGcCompleted();
      assertEquals(DynamicMemoryResizer.State.GROWING, resizer.state);

      // Verify grow step was scheduled
      verify(scheduledExecutor, atLeastOnce()).schedule(any(Runnable.class),
            eq(DynamicMemoryResizer.GROW_INITIAL_MS), eq(TimeUnit.MILLISECONDS));
   }

   @Test
   public void testShrinkCooldownSchedulesDeferredRecheck() {
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      resizer.onMemoryLow();
      verify(sharedMap).resize(800);

      // GC fires immediately — cooldown not yet elapsed
      gcGen.incrementAndGet();
      when(sharedMap.capacity()).thenReturn(800L);
      resizer.onGcCompleted();

      // Should NOT have shrunk again
      verify(sharedMap, never()).resize(600);
      // Should have scheduled a deferred recheck
      verify(scheduledExecutor).schedule(any(Runnable.class), anyLong(), eq(TimeUnit.MILLISECONDS));
   }

   @Test
   public void testGrowingToShrinkingCancelsTask() {
      when(memoryMonitor.isMemoryLow()).thenReturn(false);
      when(memoryMonitor.isGcPressureExceeded()).thenReturn(false);

      // Enter GROWING state
      resizer.state = DynamicMemoryResizer.State.GROWING;

      // Capture the scheduled future
      ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);
      verify(scheduledExecutor, never()).schedule(captor.capture(), anyLong(), any());

      // Now pressure returns
      when(memoryMonitor.isMemoryLow()).thenReturn(true);
      when(sharedMap.capacity()).thenReturn(800L);
      resizer.transitionToShrinking();

      assertEquals(DynamicMemoryResizer.State.SHRINKING, resizer.state);
      verify(sharedMap).resize(600);
   }
}
