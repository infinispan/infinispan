package org.infinispan.factories.impl;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.time.TimeService;

/**
 * Tracked state for a single component in the registry.
 *
 * <p>
 * Stores the component's current lifecycle state, timing information, and the dependency chain that triggered its initialization.
 * </p>
 *
 * @since 16.1
 */
final class ComponentEntry {
   private final String name;
   private final ComponentTiming timing;
   private final Collection<String> triggeredBy;
   private volatile BasicComponentRegistryImpl.WrapperState state;

   public ComponentEntry(String name, BasicComponentRegistryImpl.WrapperState state, List<String> triggeredBy) {
      this.name = name;
      this.state = state;
      this.triggeredBy = triggeredBy != null ? Collections.unmodifiableCollection(triggeredBy) : Collections.emptyList();
      this.timing = new ComponentTiming();
   }

   public String name() {
      return name;
   }

   public BasicComponentRegistryImpl.WrapperState state() {
      return state;
   }

   /**
    * Transition the current state of this component.
    *
    * @param state The current state the component is.
    * @param timeService The time service to update the timing information.
    */
   public void state(BasicComponentRegistryImpl.WrapperState state, TimeService timeService) {
      this.state = state;
      if (timeService != null) {
         recordTime(state, timeService.time());
      }
   }

   public long duration(TimeUnit unit) {
      return timing.duration(unit);
   }

   public Collection<String> triggeredBy() {
      return triggeredBy;
   }

   private void recordTime(BasicComponentRegistryImpl.WrapperState state, long time) {
      switch (state) {
         case INSTANTIATING -> timing.recordInstantiating(time);
         case INSTANTIATED -> timing.recordInstantiated(time);
         case WIRING -> timing.recordWiring(time);
         case WIRED -> timing.recordWired(time);
         case STARTING -> timing.recordStarting(time);
         case STARTED -> timing.recordStarted(time);
         default -> { }
      }
   }

   /**
    * Tracks timestamps for each lifecycle phase of a component.
    *
    * <p>
    * Not every component goes through all phases. Aliases and components without metadata skip WIRED and STARTING states.
    * When a phase was skipped, duration calculation falls back to the previous phase's timestamp.
    * </p>
    *
    * @since 16.2
    * @author José Bolina
    * @see ComponentRegistryTracker
    */
   private static final class ComponentTiming {
      private static final long NOT_RECORDED = -1;

      private long instantiating = NOT_RECORDED;
      private long instantiated = NOT_RECORDED;
      private long wiring = NOT_RECORDED;
      private long wired = NOT_RECORDED;
      private long starting = NOT_RECORDED;
      private long started = NOT_RECORDED;

      ComponentTiming() { }

      public void recordInstantiating(long time) {
         instantiating = time;
      }

      public void recordInstantiated(long time) {
         instantiated = time;
      }

      public void recordWiring(long time) {
         wiring = time;
      }

      public void recordWired(long time) {
         wired = time;
      }

      public void recordStarting(long time) {
         starting = time;
      }

      public void recordStarted(long time) {
         started = time;
      }

      /**
       * Duration in the given time unit from instantiating to the current/final recorded phase.
       *
       * @param unit The time unit of the output value.
       * @return -1 if not recorded. Otherwise, the time in the provided unit from instantiating to now.
       */
      public long duration(TimeUnit unit) {
         if (instantiating == NOT_RECORDED) return NOT_RECORDED;

         long end = latestRecordedTime();
         if (end == NOT_RECORDED || end == instantiating) return NOT_RECORDED;

         return unit.convert(end - instantiating, TimeUnit.NANOSECONDS);
      }

      public boolean hasRecordedTime() {
         return instantiating != NOT_RECORDED;
      }

      private long latestRecordedTime() {
         if (started != NOT_RECORDED) return started;
         if (starting != NOT_RECORDED) return starting;
         if (wired != NOT_RECORDED) return wired;
         if (wiring != NOT_RECORDED) return wiring;
         if (instantiated != NOT_RECORDED) return instantiated;
         return instantiating;
      }
   }
}
