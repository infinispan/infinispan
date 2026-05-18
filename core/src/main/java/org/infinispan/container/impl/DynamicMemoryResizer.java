package org.infinispan.container.impl;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.util.MemoryMonitor;
import org.infinispan.configuration.global.ContainerMemoryConfiguration;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.factories.KnownComponentNames;
import org.infinispan.factories.annotations.ComponentName;
import org.infinispan.factories.annotations.Inject;
import org.infinispan.factories.annotations.Start;
import org.infinispan.factories.annotations.Stop;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

@Scope(Scopes.GLOBAL)
public class DynamicMemoryResizer implements MemoryMonitor.Listener {

   enum State { STABLE, SHRINKING, GROWING }

   static final double SHRINK_FRACTION = 0.20;
   static final double GROW_FRACTION = 0.10;
   static final double FLOOR_FRACTION = 0.10;
   static final long SHRINK_COOLDOWN_MS = 5_000;
   static final long GROW_INITIAL_MS = 10_000;
   static final long GROW_MAX_MS = 30_000;

   record ContainerState(SharedCaffeineMap<?, ?> map, String name, long originalCapacity) {}

   @Inject GlobalConfiguration globalConfiguration;
   @Inject SharedContainerMaps sharedContainerMaps;
   @Inject MemoryMonitor memoryMonitor;
   @Inject @ComponentName(KnownComponentNames.TIMEOUT_SCHEDULE_EXECUTOR)
   ScheduledExecutorService scheduledExecutor;

   volatile State state = State.STABLE;
   private long shrinkGeneration;
   long lastShrinkTimeMs;
   final List<ContainerState> managedContainers = new ArrayList<>();
   private ScheduledFuture<?> currentTask;
   long currentGrowDelayMs = GROW_INITIAL_MS;

   @Start
   void start() {
      Map<String, ContainerMemoryConfiguration> configs = globalConfiguration.getMemoryContainer();
      Map<String, SharedCaffeineMap<Object, Object>> maps = sharedContainerMaps.getMaps();

      for (Map.Entry<String, ContainerMemoryConfiguration> entry : configs.entrySet()) {
         if (entry.getValue().dynamicResize()) {
            String name = entry.getKey();
            SharedCaffeineMap<Object, Object> map = maps.get(name);
            if (map != null) {
               long originalCapacity = map.capacity();
               managedContainers.add(new ContainerState(map, name, originalCapacity));
            }
         }
      }

      if (!managedContainers.isEmpty()) {
         memoryMonitor.addListener(this, scheduledExecutor);
      }
   }

   @Stop
   void stop() {
      cancelCurrentTask();
      memoryMonitor.removeListener(this);
      managedContainers.clear();
      state = State.STABLE;
   }

   @Override
   public void onMemoryLow() {
      transitionToShrinking();
   }

   @Override
   public void onGcPressureHigh() {
      transitionToShrinking();
   }

   @Override
   public void onMemoryRecovered() {
      checkRecovery();
   }

   @Override
   public void onGcPressureRelieved() {
      checkRecovery();
   }

   @Override
   public synchronized void onGcCompleted() {
      if (state == State.SHRINKING && memoryMonitor.getGcGeneration() > shrinkGeneration) {
         long elapsed = System.currentTimeMillis() - lastShrinkTimeMs;
         if (elapsed >= SHRINK_COOLDOWN_MS) {
            recheckShrink();
         } else if (currentTask == null) {
            long remaining = SHRINK_COOLDOWN_MS - elapsed;
            currentTask = scheduledExecutor.schedule(this::deferredRecheckShrink,
                  remaining, TimeUnit.MILLISECONDS);
         }
      }
   }

   synchronized void transitionToShrinking() {
      cancelCurrentTask();
      state = State.SHRINKING;
      currentGrowDelayMs = GROW_INITIAL_MS;
      shrinkStep();
   }

   synchronized void checkRecovery() {
      if (!memoryMonitor.isMemoryLow() && !memoryMonitor.isGcPressureExceeded()) {
         if (state == State.SHRINKING) {
            cancelCurrentTask();
            state = State.GROWING;
            currentGrowDelayMs = GROW_INITIAL_MS;
            scheduleGrowStep();
         }
      }
   }

   private void shrinkStep() {
      lastShrinkTimeMs = System.currentTimeMillis();
      boolean atFloor = false;
      for (ContainerState cs : managedContainers) {
         long current = cs.map.capacity();
         long floor = Math.max(1, (long) (cs.originalCapacity * FLOOR_FRACTION));
         if (current <= floor) {
            atFloor = true;
            break;
         }
         long shrinkAmount = (long) (cs.originalCapacity * SHRINK_FRACTION);
         long newCapacity = Math.max(current - shrinkAmount, floor);
         cs.map.resize(newCapacity);
         CONTAINER.containerResized(cs.name, current, newCapacity);
         if (newCapacity <= floor) {
            atFloor = true;
         }
      }
      if (atFloor) {
         CONTAINER.containerAtFloor();
      } else {
         shrinkGeneration = memoryMonitor.getGcGeneration();
      }
   }

   synchronized void deferredRecheckShrink() {
      currentTask = null;
      if (state == State.SHRINKING) {
         recheckShrink();
      }
   }

   synchronized void recheckShrink() {
      if (state != State.SHRINKING) return;
      if (memoryMonitor.isMemoryLow() || memoryMonitor.isGcPressureExceeded()) {
         shrinkStep();
      } else {
         state = State.GROWING;
         currentGrowDelayMs = GROW_INITIAL_MS;
         scheduleGrowStep();
      }
   }

   private void scheduleGrowStep() {
      currentTask = scheduledExecutor.schedule(this::growStep,
            currentGrowDelayMs, TimeUnit.MILLISECONDS);
   }

   synchronized void growStep() {
      if (state != State.GROWING) return;

      if (memoryMonitor.isMemoryLow() || memoryMonitor.isGcPressureExceeded()) {
         transitionToShrinking();
         return;
      }

      boolean fullyRecovered = true;
      for (ContainerState cs : managedContainers) {
         long current = cs.map.capacity();
         if (current >= cs.originalCapacity) continue;
         long growAmount = (long) (cs.originalCapacity * GROW_FRACTION);
         long newCapacity = Math.min(current + growAmount, cs.originalCapacity);
         cs.map.resize(newCapacity);
         CONTAINER.containerResized(cs.name, current, newCapacity);
         if (newCapacity < cs.originalCapacity) {
            fullyRecovered = false;
         }
      }

      if (fullyRecovered) {
         state = State.STABLE;
         currentGrowDelayMs = GROW_INITIAL_MS;
         CONTAINER.allContainersRecovered();
      } else {
         currentGrowDelayMs = Math.min(currentGrowDelayMs * 2, GROW_MAX_MS);
         scheduleGrowStep();
      }
   }

   private void cancelCurrentTask() {
      if (currentTask != null) {
         currentTask.cancel(false);
         currentTask = null;
      }
   }
}
