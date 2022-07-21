package org.infinispan.api.common.events.cache;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

import org.infinispan.api.async.BackpressureStrategy;
import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.Flags;

/**
 * Common Cache Listener operations
 *
 * @since 14.0
 **/
public interface CacheListenerOptions extends CacheOptions {
   CacheListenerOptions DEFAULT = new Impl();

   boolean includeCurrentState();

   Set<CacheEntryEventType> eventTypes();

   BackpressureStrategy backpressureStrategy();

   static Builder builder() {
      return new CacheListenerOptions.Builder();
   }

   static <E> Set<E> commonCacheEventTypes() {
      return (Set<E>) EnumSet.allOf(CommonCacheEventTypes.class);
   }

   class Impl extends CacheOptions.Impl implements CacheListenerOptions {
      private final boolean includeCurrentState;
      private final Set<CacheEntryEventType> eventTypes;
      private final BackpressureStrategy backpressureStrategy;

      protected Impl() {
         this(null, null, false, commonCacheEventTypes(), BackpressureStrategy.BUFFER);
      }

      protected Impl(Duration timeout, Flags<?, ?> flags, boolean includeCurrentState, Set<CacheEntryEventType> eventTypes,
            BackpressureStrategy backpressureStrategy) {
         super(timeout, flags);
         this.includeCurrentState = includeCurrentState;
         this.eventTypes = Objects.requireNonNull(eventTypes);
         if (eventTypes.isEmpty()) {
            throw new IllegalArgumentException("At least one eventType must be supplied!");
         }
         this.backpressureStrategy = backpressureStrategy;
      }

      @Override
      public boolean includeCurrentState() {
         return includeCurrentState;
      }

      @Override
      public Set<CacheEntryEventType> eventTypes() {
         return eventTypes;
      }

      @Override
      public BackpressureStrategy backpressureStrategy() {
         return backpressureStrategy;
      }
   }

   class Builder extends CacheOptions.Builder {
      protected boolean includeCurrentState;
      protected Set<CacheEntryEventType> eventTypes;
      protected BackpressureStrategy backpressureStrategy = BackpressureStrategy.BUFFER;

      public Builder includeCurrentState(boolean includeCurrentState) {
         this.includeCurrentState = includeCurrentState;
         return this;
      }

      @Override
      public CacheListenerOptions.Builder timeout(Duration timeout) {
         super.timeout(timeout);
         return this;
      }

      @Override
      public CacheListenerOptions.Builder flags(Flags<?, ?> flags) {
         super.flags(flags);
         return this;
      }

      public Builder eventTypes(CacheEntryEventType eventType, CacheEntryEventType... eventTypes) {
         if (eventTypes.length == 0) {
            this.eventTypes = Collections.singleton(eventType);
         } else {
            this.eventTypes = new HashSet<>(1 + eventTypes.length);
            this.eventTypes.add(eventType);
            this.eventTypes.addAll(Arrays.asList(eventTypes));
         }
         return this;
      }

      public Builder backpressureStrategy(BackpressureStrategy backpressureStrategy) {
         this.backpressureStrategy = Objects.requireNonNull(backpressureStrategy);
         return this;
      }

      public CacheListenerOptions build() {
         if (eventTypes == null) {
            if (!includeCurrentState && timeout == null && flags == null &&
                  backpressureStrategy == BackpressureStrategy.BUFFER) {
               return DEFAULT;
            }
            eventTypes = commonCacheEventTypes();
         }
         return new Impl(timeout, flags, includeCurrentState, eventTypes, backpressureStrategy);
      }
   }
}
