package org.infinispan.commons.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

/**
 * Integration test for {@link MemoryMonitor} that verifies memory threshold notifications
 * are triggered when the JVM is under actual memory pressure.
 * <p>
 * This test must be run in a forked JVM with low heap (e.g. -Xmx256m) so that
 * allocating memory can reliably trigger the low memory alert.
 */
public class MemoryMonitorLowMemoryTest {

   private MemoryMonitor monitor;

   @After
   public void cleanup() {
      if (monitor != null) {
         monitor.stop();
      }
   }

   @Test
   public void testLowMemoryAlertTriggered() {
      // Use a low threshold so the alert triggers easily in the constrained heap
      monitor = new MemoryMonitor(0.50, 5000, 0.20, 60_000);
      assertFalse(monitor.isMemoryLow());

      // Allocate memory to trigger the threshold notification.
      // With -Xmx256m and a 50% threshold, ~128MB of old gen usage triggers the alert.
      // The test framework uses ~40-50MB, so allocating ~100MB should cross the threshold.
      // Memory threshold notifications are only sent after a GC, so we periodically
      // trigger GC to allow the notification to fire.
      byte[][] blocks = new byte[200][];
      try {
         for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new byte[1024 * 1024]; // 1MB blocks
            if (i % 10 == 9) {
               System.gc();
               Thread.sleep(100);
            }
            if (monitor.isMemoryLow()) {
               break;
            }
         }
      } catch (OutOfMemoryError ignored) {
         // May happen in a constrained heap — the alert should already be set
      } catch (InterruptedException e) {
         Thread.currentThread().interrupt();
      }

      assertTrue("Low memory alert should have been triggered", monitor.isMemoryLow());

      // Release memory and trigger GC to verify auto-recovery
      for (int i = 0; i < blocks.length; i++) {
         blocks[i] = null;
      }
      for (int i = 0; i < 10; i++) {
         System.gc();
         try {
            Thread.sleep(500);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
         }
         if (!monitor.isMemoryLow()) {
            break;
         }
      }

      assertFalse("Low memory alert should have cleared after GC", monitor.isMemoryLow());
   }
}
