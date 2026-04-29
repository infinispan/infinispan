package org.infinispan.commons.util;

import static org.infinispan.commons.logging.Log.CONFIG;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryNotificationInfo;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.management.ListenerNotFoundException;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;

/**
 * Monitors JVM memory usage and garbage collection activity, raising alerts when configurable
 * thresholds are exceeded.
 * <p>
 * Two types of conditions are tracked:
 * <ul>
 *    <li><b>Memory threshold</b> &mdash; alerts when old generation heap usage exceeds a configured
 *    fraction of the maximum heap. The alert auto-clears after a GC if usage drops back below the
 *    threshold. Requires a JVM that exposes an old generation memory pool with usage threshold
 *    support. All collectors on JDK 25+ are supported; on older JDKs, ZGC and non-generational
 *    Shenandoah may lack this, in which case the monitor is automatically disabled and a warning
 *    is logged.</li>
 *    <li><b>GC pressure</b> &mdash; alerts when the ratio of time spent in GC over a rolling window
 *    exceeds a configured fraction. Auto-clears when pressure drops below the threshold.</li>
 * </ul>
 * <p>
 * Individual GC pauses exceeding the duration threshold are logged as warnings but do not raise
 * a persistent alert.
 * <p>
 * All thresholds are mutable at runtime. A single instance is registered per cache manager in the
 * GlobalComponentRegistry.
 *
 * @since 16.2
 */
public class MemoryMonitor {
   private final AtomicBoolean lowMemoryAlert = new AtomicBoolean(false);
   private final AtomicBoolean gcPressureAlert = new AtomicBoolean(false);

   private volatile double memoryThresholdPercentage;
   private volatile long gcDurationThresholdMs;
   private volatile double gcPressureThreshold;
   private volatile long gcPressureWindowMs;

   private record GcEvent(long timestampMs, long durationMs) {}
   private final Deque<GcEvent> gcEvents = new ConcurrentLinkedDeque<>();

   private final List<ListenerRegistration> registeredListeners = new ArrayList<>();

   private record ListenerRegistration(NotificationEmitter emitter, NotificationListener listener) {}

   private final MemoryPoolMXBean oldGenPool;

   /**
    * Creates a new monitor with the specified thresholds and immediately registers JMX listeners.
    *
    * @param memoryThresholdPercentage fraction (0, 1.0] of old gen usage that triggers a low memory alert
    * @param gcDurationThresholdMs     GC pause duration in ms above which a warning is logged
    * @param gcPressureThreshold       fraction (0, 1.0] of time spent in GC over the pressure window that triggers an alert
    * @param gcPressureWindowMs        rolling window in ms over which GC pressure is computed
    */
   public MemoryMonitor(double memoryThresholdPercentage, long gcDurationThresholdMs,
                        double gcPressureThreshold, long gcPressureWindowMs) {
      this.memoryThresholdPercentage = memoryThresholdPercentage;
      this.gcDurationThresholdMs = gcDurationThresholdMs;
      this.gcPressureThreshold = gcPressureThreshold;
      this.gcPressureWindowMs = gcPressureWindowMs;
      this.oldGenPool = findOldGenPool();
      setupMemoryListener();
      setupGcListener();
   }

   /**
    * Creates a new monitor with default thresholds: 85% memory, 5000ms GC duration,
    * 20% GC pressure over a 60s window.
    */
   public MemoryMonitor() {
      this(0.85, 5000, 0.20, 60_000);
   }

   private void setupMemoryListener() {
      if (oldGenPool == null || !oldGenPool.isUsageThresholdSupported()) {
         CONFIG.memoryThresholdMonitoringUnavailable();
         return;
      }
      applyMemoryThreshold();

      MemoryMXBean mbean = ManagementFactory.getMemoryMXBean();
      NotificationEmitter emitter = (NotificationEmitter) mbean;

      NotificationListener listener = (n, hb) -> {
         if (MemoryNotificationInfo.MEMORY_THRESHOLD_EXCEEDED.equals(n.getType())) {
            lowMemoryAlert.set(true);
            CONFIG.lowMemoryDetected();
         }
      };
      emitter.addNotificationListener(listener, null, null);
      registeredListeners.add(new ListenerRegistration(emitter, listener));
   }

   private void setupGcListener() {
      for (GarbageCollectorMXBean gcBean : ManagementFactory.getGarbageCollectorMXBeans()) {
         if (gcBean instanceof NotificationEmitter emitter) {
            NotificationListener listener = (n, hb) -> {
               if (GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(n.getType())) {
                  GarbageCollectionNotificationInfo gcInfo = GarbageCollectionNotificationInfo.from((CompositeData) n.getUserData());
                  GcInfo info = gcInfo.getGcInfo();
                  long duration = info.getDuration();
                  long now = System.currentTimeMillis();
                  if (duration >= gcDurationThresholdMs) {
                     CONFIG.gcDurationExceeded(duration, gcDurationThresholdMs);
                  }
                  boolean wasPressured = gcPressureAlert.get();
                  recordGcEvent(now, duration);
                  if (!wasPressured && gcPressureAlert.get()) {
                     CONFIG.gcPressureExceeded((int) (gcPressureThreshold * 100), gcPressureWindowMs / 1000);
                  }
                  checkMemoryRecovery();
               }
            };
            emitter.addNotificationListener(listener, null, null);
            registeredListeners.add(new ListenerRegistration(emitter, listener));
         }
      }
   }

   /**
    * Removes all registered JMX notification listeners. Called during cache manager shutdown.
    */
   public void stop() {
      for (ListenerRegistration reg : registeredListeners) {
         try {
            reg.emitter.removeNotificationListener(reg.listener);
         } catch (ListenerNotFoundException e) {
            // already removed
         }
      }
      registeredListeners.clear();
   }

   // Visible for testing
   void recordGcEvent(long timestampMs, long durationMs) {
      gcEvents.addLast(new GcEvent(timestampMs, durationMs));
      evictOldEvents(timestampMs);
      if (computeGcPressure(timestampMs) >= gcPressureThreshold) {
         gcPressureAlert.compareAndSet(false, true);
      } else {
         gcPressureAlert.set(false);
      }
   }

   private void evictOldEvents(long now) {
      long cutoff = now - gcPressureWindowMs;
      while (!gcEvents.isEmpty() && gcEvents.peekFirst().timestampMs < cutoff) {
         gcEvents.pollFirst();
      }
   }

   private double computeGcPressure(long now) {
      long windowMs = gcPressureWindowMs;
      long totalGcMs = 0;
      long cutoff = now - windowMs;
      for (GcEvent event : gcEvents) {
         if (event.timestampMs >= cutoff) {
            totalGcMs += event.durationMs;
         }
      }
      return (double) totalGcMs / windowMs;
   }

   private void checkMemoryRecovery() {
      if (lowMemoryAlert.get() && oldGenPool != null) {
         long used = oldGenPool.getUsage().getUsed();
         long max = oldGenPool.getUsage().getMax();
         if (max > 0 && (double) used / max < memoryThresholdPercentage) {
            lowMemoryAlert.set(false);
         }
      }
   }

   private void applyMemoryThreshold() {
      long maxMemory = oldGenPool.getUsage().getMax();
      if (maxMemory > 0) {
         oldGenPool.setUsageThreshold((long) (maxMemory * memoryThresholdPercentage));
      }
   }

   private static MemoryPoolMXBean findOldGenPool() {
      MemoryPoolMXBean fallback = null;
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
         if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
            String name = pool.getName();
            if (name.contains("Old") || name.contains("Tenured")) {
               return pool;
            }
            if (fallback == null) {
               fallback = pool;
            }
         }
      }
      return fallback;
   }

   /**
    * @return {@code true} if old generation heap usage has exceeded the memory threshold
    */
   public boolean isMemoryLow() {
      return lowMemoryAlert.get();
   }

   /**
    * @return {@code true} if GC time over the pressure window has exceeded the pressure threshold
    */
   public boolean isGcPressureExceeded() {
      return gcPressureAlert.get();
   }

   /**
    * Clears all alerts and discards recorded GC events.
    */
   public void reset() {
      lowMemoryAlert.set(false);
      gcPressureAlert.set(false);
      gcEvents.clear();
   }

   /**
    * Sets the old generation heap usage threshold. Clears any active low memory alert and
    * re-applies the threshold to the memory pool.
    *
    * @param percentage fraction (0, 1.0]
    * @throws org.infinispan.commons.CacheConfigurationException if the value is out of range
    */
   public void setMemoryThreshold(double percentage) {
      if (percentage <= 0 || percentage > 1.0) {
         throw CONFIG.attributeMustBeFraction(percentage, "memory-threshold");
      }
      memoryThresholdPercentage = percentage;
      lowMemoryAlert.set(false);
      if (oldGenPool != null && oldGenPool.isUsageThresholdSupported()) {
         applyMemoryThreshold();
      }
   }

   /**
    * Sets the GC pause duration threshold for log warnings.
    *
    * @param durationMs duration in milliseconds, must be positive
    * @throws org.infinispan.commons.CacheConfigurationException if the value is not positive
    */
   public void setGcDurationThreshold(long durationMs) {
      if (durationMs <= 0) {
         throw CONFIG.attributeMustBePositive(durationMs, "gc-duration-threshold");
      }
      gcDurationThresholdMs = durationMs;
   }

   /**
    * Sets the GC pressure threshold. Clears any active GC pressure alert.
    *
    * @param percentage fraction (0, 1.0]
    * @throws org.infinispan.commons.CacheConfigurationException if the value is out of range
    */
   public void setGcPressureThreshold(double percentage) {
      if (percentage <= 0 || percentage > 1.0) {
         throw CONFIG.attributeMustBeFraction(percentage, "gc-pressure-threshold");
      }
      gcPressureThreshold = percentage;
      gcPressureAlert.set(false);
   }

   /**
    * Sets the rolling window for GC pressure computation. Clears any active GC pressure alert
    * and discards all recorded GC events.
    *
    * @param windowMs window duration in milliseconds, must be positive
    * @throws org.infinispan.commons.CacheConfigurationException if the value is not positive
    */
   public void setGcPressureWindow(long windowMs) {
      if (windowMs <= 0) {
         throw CONFIG.attributeMustBePositive(windowMs, "gc-pressure-window");
      }
      gcPressureWindowMs = windowMs;
      gcPressureAlert.set(false);
      gcEvents.clear();
   }

   public double getMemoryThreshold() {
      return memoryThresholdPercentage;
   }

   public long getGcDurationThreshold() {
      return gcDurationThresholdMs;
   }

   public double getGcPressureThreshold() {
      return gcPressureThreshold;
   }

   public long getGcPressureWindow() {
      return gcPressureWindowMs;
   }
}
