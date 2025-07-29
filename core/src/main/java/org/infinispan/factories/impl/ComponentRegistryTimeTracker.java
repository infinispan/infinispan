package org.infinispan.factories.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.jgroups.util.Util.printTime;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;

import org.infinispan.commons.time.TimeService;
import org.infinispan.configuration.global.GlobalConfiguration;

final class ComponentRegistryTimeTracker implements ComponentRegistryTracker {

   private final Map<String, Initialization> components = new ConcurrentHashMap<>(8);
   private final BasicComponentRegistry registry;
   private volatile TimeService timeService;

   ComponentRegistryTimeTracker(BasicComponentRegistry registry, boolean global) {
      this.registry = registry;
      this.timeService = global ? null : registry.getComponent(TimeService.class).wired();
   }

   static ComponentRegistryTracker tracker(BasicComponentRegistry registry, boolean global) {
      return new ComponentRegistryTimeTracker(registry, global);
   }

   private boolean isTimeServiceAvailable() {
      return timeService != null;
   }

   @Override
   public void instantiating(String componentName) {
      if (!isTimeServiceAvailable()) return;
      Initialization initialization = componentInitialization(componentName);
      initialization.instantiating(timeService.wallClockTime());
   }

   @Override
   public void instantiated(String componentName) {
      if (!isTimeServiceAvailable()) return;
      Initialization initialization = componentInitialization(componentName);
      initialization.instantiated(timeService.wallClockTime());
   }

   @Override
   public void wiring(String componentName) {
      if (!isTimeServiceAvailable()) return;
      Initialization initialization = componentInitialization(componentName);
      initialization.wiring(timeService.wallClockTime());
   }

   @Override
   public void wired(String componentName) {
      if (!isTimeServiceAvailable()) {
         if (componentName.equals(GlobalConfiguration.class.getName())) {
            timeService = registry.getComponent(TimeService.class).wired();
         }
         return;
      }
      Initialization initialization = componentInitialization(componentName);
      initialization.wired(timeService.wallClockTime());
   }

   @Override
   public void starting(String componentName) {
      if (!isTimeServiceAvailable()) return;
      Initialization initialization = componentInitialization(componentName);
      initialization.starting(timeService.wallClockTime());
   }

   @Override
   public void started(String componentName) {
      if (!isTimeServiceAvailable()) return;
      Initialization initialization = componentInitialization(componentName);
      initialization.started(timeService.wallClockTime());
   }

   @Override
   public String dump() {
      StringBuilder sb = new StringBuilder();
      List<Initialization> c = components.values().stream()
            .sorted(Comparator.reverseOrder())
            .toList();
      for (Initialization initialization : c) {
         if (!initialization.isValid())
            continue;
         sb.append(initialization.name()).append(":").append(System.lineSeparator());
         sb.append(initialization);
         sb.append(System.lineSeparator());
      }
      return sb.toString();
   }

   @Override
   public void clear() {
      components.clear();
   }

   @Override
   public void removeComponent(String componentName) {
      components.remove(componentName);
   }

   private Initialization componentInitialization(String componentName) {
      return components.computeIfAbsent(componentName, Initialization::new);
   }

   private static final class Initialization implements Comparable<Initialization> {
      private static final long NOT_INITIALIZED = -1L;
      private final String name;
      private static final AtomicLongFieldUpdater<Initialization> INSTANTIATING = AtomicLongFieldUpdater.newUpdater(Initialization.class, "instantiating");
      private static final AtomicLongFieldUpdater<Initialization> INSTANTIATED = AtomicLongFieldUpdater.newUpdater(Initialization.class, "instantiated");
      private static final AtomicLongFieldUpdater<Initialization> WIRING = AtomicLongFieldUpdater.newUpdater(Initialization.class, "wiring");
      private static final AtomicLongFieldUpdater<Initialization> WIRED = AtomicLongFieldUpdater.newUpdater(Initialization.class, "wired");
      private static final AtomicLongFieldUpdater<Initialization> STARTING = AtomicLongFieldUpdater.newUpdater(Initialization.class, "starting");
      private static final AtomicLongFieldUpdater<Initialization> STARTED = AtomicLongFieldUpdater.newUpdater(Initialization.class, "started");

      private volatile long instantiating = NOT_INITIALIZED;
      private volatile long instantiated = NOT_INITIALIZED;
      private volatile long wiring = NOT_INITIALIZED;
      private volatile long wired = NOT_INITIALIZED;
      private volatile long starting = NOT_INITIALIZED;
      private volatile long started = NOT_INITIALIZED;

      private Initialization(String name) {
         this.name = name;
      }

      public String name() {
         return name;
      }

      public boolean isValid() {
         return INSTANTIATING.get(this) != NOT_INITIALIZED && STARTED.get(this) != NOT_INITIALIZED;
      }

      public void instantiating(long value) {
         if (!INSTANTIATING.compareAndSet(this, NOT_INITIALIZED, value)) {
            throw new IllegalStateException("Field already initialized: " + INSTANTIATING.get(this));
         }
      }

      public long instantiatingTime() {
         return getInitializedValue(BasicComponentRegistryImpl.WrapperState.INSTANTIATED) - INSTANTIATING.get(this);
      }

      public void instantiated(long value) {
         if (!INSTANTIATED.compareAndSet(this, NOT_INITIALIZED, value)) {
            throw new IllegalStateException("Field already initialized: " + INSTANTIATED.get(this));
         }
      }

      public long instantiatedTime() {
         return getInitializedValue(BasicComponentRegistryImpl.WrapperState.WIRING) - getInitializedValue(BasicComponentRegistryImpl.WrapperState.INSTANTIATED);
      }

      public void wiring(long value) {
         if (!WIRING.compareAndSet(this, NOT_INITIALIZED, value)) {
            throw new IllegalStateException("Field already initialized: " + WIRING.get(this));
         }
      }

      public long wiringTime() {
         return getInitializedValue(BasicComponentRegistryImpl.WrapperState.WIRED) - getInitializedValue(BasicComponentRegistryImpl.WrapperState.WIRING);
      }

      public void wired(long value) {
         if (!WIRED.compareAndSet(this, NOT_INITIALIZED, value)) {
            throw new IllegalStateException("Field already initialized: " + WIRED.get(this));
         }
      }

      public long wiredTime() {
         return getInitializedValue(BasicComponentRegistryImpl.WrapperState.STARTING) - getInitializedValue(BasicComponentRegistryImpl.WrapperState.WIRED);
      }

      public void starting(long value) {
         if (!STARTING.compareAndSet(this, NOT_INITIALIZED, value)) {
            throw new IllegalStateException("Field already initialized: " + STARTING.get(this));
         }
      }

      public long startingTime() {
         return getInitializedValue(BasicComponentRegistryImpl.WrapperState.STARTED) - getInitializedValue(BasicComponentRegistryImpl.WrapperState.STARTING);
      }

      public void started(long value) {
         if (!STARTED.compareAndSet(this, NOT_INITIALIZED, value)) {
            throw new IllegalStateException("Field already initialized: " + STARTED.get(this));
         }
      }

      private long totalTime() {
         return getInitializedValue(BasicComponentRegistryImpl.WrapperState.STARTED) - INSTANTIATING.get(this);
      }

      private long getInitializedValue(BasicComponentRegistryImpl.WrapperState state) {
         return switch (state) {
            case INSTANTIATING -> INSTANTIATING.get(this);
            case INSTANTIATED -> {
               long value = INSTANTIATED.get(this);
               yield value == NOT_INITIALIZED
                     ? getInitializedValue(BasicComponentRegistryImpl.WrapperState.INSTANTIATING)
                     : value;
            }
            case WIRING -> {
               long value = WIRING.get(this);
               yield value == NOT_INITIALIZED
                     ? getInitializedValue(BasicComponentRegistryImpl.WrapperState.INSTANTIATED)
                     : value;
            }
            case WIRED -> {
               long value = WIRED.get(this);
               yield value == NOT_INITIALIZED
                     ? getInitializedValue(BasicComponentRegistryImpl.WrapperState.WIRING)
                     : value;
            }
            case STARTING -> {
               long value = STARTING.get(this);
               yield value == NOT_INITIALIZED
                     ? getInitializedValue(BasicComponentRegistryImpl.WrapperState.WIRED)
                     : value;
            }
            case STARTED -> {
               long value = STARTED.get(this);
               yield value == NOT_INITIALIZED
                     ? getInitializedValue(BasicComponentRegistryImpl.WrapperState.STARTING)
                     : value;
            }
            default -> NOT_INITIALIZED;
         };
      }

      @Override
      public String toString() {
         StringBuilder sb = new StringBuilder();
         append(sb, "\t-> instantiating", instantiatingTime());
         append(sb, "\t->  instantiated", instantiatedTime());
         append(sb, "\t->        wiring", wiringTime());
         append(sb, "\t->         wired", wiredTime());
         append(sb, "\t->      starting", startingTime());
         sb.append('\t').append("->         total: ").append(printTime(totalTime(), MILLISECONDS));
         return sb.toString();
      }

      private void append(StringBuilder sb, String field, long value) {
         if (value > 0)
            sb.append(field).append(": ").append(printTime(value, MILLISECONDS)).append(System.lineSeparator());
      }

      @Override
      public int compareTo(Initialization initialization) {
         int time = Long.compare(totalTime(), initialization.totalTime());
         return time == 0
               ? name.compareTo(initialization.name())
               : time;
      }
   }
}
