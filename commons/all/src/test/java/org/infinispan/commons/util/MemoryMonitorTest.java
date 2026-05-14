package org.infinispan.commons.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.atomic.AtomicInteger;

import org.infinispan.commons.CacheConfigurationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class MemoryMonitorTest {

   private MemoryMonitor monitor;

   @BeforeEach
   public void setup() {
      monitor = new MemoryMonitor();
   }

   @AfterEach
   public void cleanup() {
      monitor.stop();
   }

   @Test
   public void testDefaultThresholds() {
      assertEquals(0.85, monitor.getMemoryThreshold(), 0.001);
      assertEquals(5000, monitor.getGcDurationThreshold());
      assertEquals(0.20, monitor.getGcPressureThreshold(), 0.001);
      assertEquals(60_000, monitor.getGcPressureWindow());
   }

   @Test
   public void testCustomThresholds() {
      MemoryMonitor custom = new MemoryMonitor(0.90, 3000, 0.10, 30_000);
      try {
         assertEquals(0.90, custom.getMemoryThreshold(), 0.001);
         assertEquals(3000, custom.getGcDurationThreshold());
         assertEquals(0.10, custom.getGcPressureThreshold(), 0.001);
         assertEquals(30_000, custom.getGcPressureWindow());
      } finally {
         custom.stop();
      }
   }

   @Test
   public void testSetMemoryThreshold() {
      monitor.setMemoryThreshold(0.50);
      assertEquals(0.50, monitor.getMemoryThreshold(), 0.001);
   }

   @Test
   public void testSetMemoryThresholdInvalid() {
      assertThrows(CacheConfigurationException.class, () -> monitor.setMemoryThreshold(0));
      assertThrows(CacheConfigurationException.class, () -> monitor.setMemoryThreshold(-0.5));
      assertThrows(CacheConfigurationException.class, () -> monitor.setMemoryThreshold(1.5));
   }

   @Test
   public void testSetGcDurationThreshold() {
      monitor.setGcDurationThreshold(1000);
      assertEquals(1000, monitor.getGcDurationThreshold());
   }

   @Test
   public void testSetGcDurationThresholdInvalid() {
      assertThrows(CacheConfigurationException.class, () -> monitor.setGcDurationThreshold(0));
      assertThrows(CacheConfigurationException.class, () -> monitor.setGcDurationThreshold(-100));
   }

   @Test
   public void testSetGcPressureThreshold() {
      monitor.setGcPressureThreshold(0.30);
      assertEquals(0.30, monitor.getGcPressureThreshold(), 0.001);
   }

   @Test
   public void testSetGcPressureThresholdInvalid() {
      assertThrows(CacheConfigurationException.class, () -> monitor.setGcPressureThreshold(0));
      assertThrows(CacheConfigurationException.class, () -> monitor.setGcPressureThreshold(1.5));
   }

   @Test
   public void testSetGcPressureWindow() {
      monitor.setGcPressureWindow(30_000);
      assertEquals(30_000, monitor.getGcPressureWindow());
   }

   @Test
   public void testSetGcPressureWindowInvalid() {
      assertThrows(CacheConfigurationException.class, () -> monitor.setGcPressureWindow(0));
      assertThrows(CacheConfigurationException.class, () -> monitor.setGcPressureWindow(-1));
   }

   @Test
   public void testGcPressureAlert() {
      // 10-second window, 20% threshold => 2000ms of GC in 10s triggers alert
      monitor.setGcPressureWindow(10_000);
      monitor.setGcPressureThreshold(0.20);
      assertFalse(monitor.isGcPressureExceeded());

      long now = 100_000;
      // Add 1500ms of GC over 10s => 15%, below threshold
      monitor.recordGcEvent(now - 8000, 500);
      monitor.recordGcEvent(now - 4000, 500);
      monitor.recordGcEvent(now, 500);
      assertFalse(monitor.isGcPressureExceeded());

      // Add another 1000ms => 2500ms total => 25%, above threshold
      monitor.recordGcEvent(now + 1000, 1000);
      assertTrue(monitor.isGcPressureExceeded());
   }

   @Test
   public void testGcPressureEviction() {
      // 5-second window
      monitor.setGcPressureWindow(5000);
      monitor.setGcPressureThreshold(0.20);

      long now = 100_000;
      // Record heavy GC in the past
      monitor.recordGcEvent(now - 10_000, 2000);
      // Old event should be evicted, no pressure
      monitor.recordGcEvent(now, 100);
      assertFalse(monitor.isGcPressureExceeded());
   }

   @Test
   public void testGcPressureAutoClears() {
      monitor.setGcPressureWindow(10_000);
      monitor.setGcPressureThreshold(0.20);

      long now = 100_000;
      // Trigger pressure
      monitor.recordGcEvent(now, 3000);
      assertTrue(monitor.isGcPressureExceeded());

      // Record a light event well after the window has passed — old event evicted
      monitor.recordGcEvent(now + 20_000, 10);
      assertFalse(monitor.isGcPressureExceeded());
   }

   @Test
   public void testResetClearsAllAlerts() {
      monitor.setGcPressureWindow(10_000);
      monitor.setGcPressureThreshold(0.20);
      monitor.recordGcEvent(100_000, 3000);
      assertTrue(monitor.isGcPressureExceeded());

      monitor.reset();
      assertFalse(monitor.isMemoryLow());
      assertFalse(monitor.isGcPressureExceeded());
   }

   @Test
   public void testListenerGcPressureHighCallback() {
      monitor.setGcPressureWindow(10_000);
      monitor.setGcPressureThreshold(0.20);

      AtomicInteger highCount = new AtomicInteger();
      monitor.addListener(new CountingListener(null, null, highCount, null), Runnable::run);

      monitor.recordGcEvent(100_000, 3000);
      assertEquals(1, highCount.get());

      // Second event while still pressured should not re-fire
      monitor.recordGcEvent(101_000, 3000);
      assertEquals(1, highCount.get());
   }

   @Test
   public void testListenerGcPressureRelievedCallback() {
      monitor.setGcPressureWindow(10_000);
      monitor.setGcPressureThreshold(0.20);

      AtomicInteger relievedCount = new AtomicInteger();
      monitor.addListener(new CountingListener(null, null, null, relievedCount), Runnable::run);

      // Trigger pressure
      monitor.recordGcEvent(100_000, 3000);
      assertTrue(monitor.isGcPressureExceeded());

      // Pressure clears after window passes
      monitor.recordGcEvent(120_000, 10);
      assertFalse(monitor.isGcPressureExceeded());
      assertEquals(1, relievedCount.get());
   }

   @Test
   public void testListenerMemoryLowCallback() {
      AtomicInteger lowCount = new AtomicInteger();
      monitor.addListener(new CountingListener(lowCount, null, null, null), Runnable::run);

      monitor.simulateMemoryLow();
      assertTrue(monitor.isMemoryLow());
      assertEquals(1, lowCount.get());

      // Second call should not re-fire
      monitor.simulateMemoryLow();
      assertEquals(1, lowCount.get());
   }

   @Test
   public void testListenerMemoryRecoveredCallback() {
      AtomicInteger recoveredCount = new AtomicInteger();
      monitor.addListener(new CountingListener(null, recoveredCount, null, null), Runnable::run);

      monitor.simulateMemoryLow();
      assertTrue(monitor.isMemoryLow());

      monitor.simulateMemoryRecovered();
      assertFalse(monitor.isMemoryLow());
      assertEquals(1, recoveredCount.get());
   }

   @Test
   public void testRemoveListener() {
      AtomicInteger count = new AtomicInteger();
      MemoryMonitor.Listener listener = new CountingListener(count, null, null, null);

      monitor.addListener(listener, Runnable::run);
      monitor.simulateMemoryLow();
      assertEquals(1, count.get());

      monitor.removeListener(listener);
      monitor.simulateMemoryRecovered();
      monitor.simulateMemoryLow();
      assertEquals(1, count.get());
   }

   @Test
   public void testGcCompletedCallback() {
      AtomicInteger completedCount = new AtomicInteger();
      monitor.addListener(new CountingListener(null, null, null, null, completedCount), Runnable::run);

      monitor.recordGcEvent(100_000, 100);
      monitor.recordGcEvent(101_000, 200);
      monitor.recordGcEvent(102_000, 50);
      assertEquals(3, completedCount.get());
   }

   private static class CountingListener implements MemoryMonitor.Listener {
      private final AtomicInteger memoryLow;
      private final AtomicInteger memoryRecovered;
      private final AtomicInteger gcPressureHigh;
      private final AtomicInteger gcPressureRelieved;
      private final AtomicInteger gcCompleted;

      CountingListener(AtomicInteger memoryLow, AtomicInteger memoryRecovered,
                       AtomicInteger gcPressureHigh, AtomicInteger gcPressureRelieved) {
         this(memoryLow, memoryRecovered, gcPressureHigh, gcPressureRelieved, null);
      }

      CountingListener(AtomicInteger memoryLow, AtomicInteger memoryRecovered,
                       AtomicInteger gcPressureHigh, AtomicInteger gcPressureRelieved,
                       AtomicInteger gcCompleted) {
         this.memoryLow = memoryLow;
         this.memoryRecovered = memoryRecovered;
         this.gcPressureHigh = gcPressureHigh;
         this.gcPressureRelieved = gcPressureRelieved;
         this.gcCompleted = gcCompleted;
      }

      @Override
      public void onMemoryLow() {
         if (memoryLow != null) memoryLow.incrementAndGet();
      }

      @Override
      public void onMemoryRecovered() {
         if (memoryRecovered != null) memoryRecovered.incrementAndGet();
      }

      @Override
      public void onGcPressureHigh() {
         if (gcPressureHigh != null) gcPressureHigh.incrementAndGet();
      }

      @Override
      public void onGcPressureRelieved() {
         if (gcPressureRelieved != null) gcPressureRelieved.incrementAndGet();
      }

      @Override
      public void onGcCompleted() {
         if (gcCompleted != null) gcCompleted.incrementAndGet();
      }
   }
}
