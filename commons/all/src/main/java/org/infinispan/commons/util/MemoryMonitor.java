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

public class MemoryMonitor {
   private final AtomicBoolean lowMemoryAlert = new AtomicBoolean(false);
   private final AtomicBoolean gcDurationAlert = new AtomicBoolean(false);
   private final AtomicBoolean gcPressureAlert = new AtomicBoolean(false);

   private volatile double memoryThresholdPercentage;
   private volatile long gcDurationThresholdMs;
   private volatile double gcPressureThreshold;
   private volatile long gcPressureWindowMs;

   private record GcEvent(long timestampMs, long durationMs) {}
   private final Deque<GcEvent> gcEvents = new ConcurrentLinkedDeque<>();

   private final List<ListenerRegistration> registeredListeners = new ArrayList<>();

   private record ListenerRegistration(NotificationEmitter emitter, NotificationListener listener) {}

   public MemoryMonitor(double memoryThresholdPercentage, long gcDurationThresholdMs,
                        double gcPressureThreshold, long gcPressureWindowMs) {
      this.memoryThresholdPercentage = memoryThresholdPercentage;
      this.gcDurationThresholdMs = gcDurationThresholdMs;
      this.gcPressureThreshold = gcPressureThreshold;
      this.gcPressureWindowMs = gcPressureWindowMs;
      setupMemoryListener();
      setupGcListener();
   }

   public MemoryMonitor() {
      this(0.85, 5000, 0.20, 60_000);
   }

   private void setupMemoryListener() {
      MemoryPoolMXBean oldGenPool = findOldGenPool();
      if (oldGenPool != null && oldGenPool.isUsageThresholdSupported()) {
         applyMemoryThreshold(oldGenPool);

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
                  recordGcEvent(now, duration);
                  if (gcPressureAlert.get()) {
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
      if (durationMs >= gcDurationThresholdMs) {
         gcDurationAlert.set(true);
      }
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
      if (lowMemoryAlert.get()) {
         MemoryPoolMXBean oldGenPool = findOldGenPool();
         if (oldGenPool != null) {
            long used = oldGenPool.getUsage().getUsed();
            long max = oldGenPool.getUsage().getMax();
            if (max > 0 && (double) used / max < memoryThresholdPercentage) {
               lowMemoryAlert.set(false);
            }
         }
      }
   }

   private void applyMemoryThreshold(MemoryPoolMXBean pool) {
      long maxMemory = pool.getUsage().getMax();
      if (maxMemory > 0) {
         pool.setUsageThreshold((long) (maxMemory * memoryThresholdPercentage));
      }
   }

   private static MemoryPoolMXBean findOldGenPool() {
      for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
         if (pool.getType() == MemoryType.HEAP && pool.isUsageThresholdSupported()) {
            return pool;
         }
      }
      return null;
   }

   public boolean isMemoryLow() {
      return lowMemoryAlert.get();
   }

   public boolean isGcDurationExceeded() {
      return gcDurationAlert.get();
   }

   public boolean isGcPressureExceeded() {
      return gcPressureAlert.get();
   }

   public void reset() {
      lowMemoryAlert.set(false);
      gcDurationAlert.set(false);
      gcPressureAlert.set(false);
      gcEvents.clear();
   }

   public void setMemoryThreshold(double percentage) {
      if (percentage <= 0 || percentage > 1.0) {
         throw CONFIG.attributeMustBeFraction(percentage, "memory-threshold");
      }
      memoryThresholdPercentage = percentage;
      lowMemoryAlert.set(false);
      MemoryPoolMXBean oldGenPool = findOldGenPool();
      if (oldGenPool != null && oldGenPool.isUsageThresholdSupported()) {
         applyMemoryThreshold(oldGenPool);
      }
   }

   public void setGcDurationThreshold(long durationMs) {
      if (durationMs <= 0) {
         throw CONFIG.attributeMustBePositive(durationMs, "gc-duration-threshold");
      }
      gcDurationThresholdMs = durationMs;
      gcDurationAlert.set(false);
   }

   public void setGcPressureThreshold(double percentage) {
      if (percentage <= 0 || percentage > 1.0) {
         throw CONFIG.attributeMustBeFraction(percentage, "gc-pressure-threshold");
      }
      gcPressureThreshold = percentage;
      gcPressureAlert.set(false);
   }

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
