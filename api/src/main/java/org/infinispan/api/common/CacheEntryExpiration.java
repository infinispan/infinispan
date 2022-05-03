package org.infinispan.api.common;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

/**
 * @since 14.0
 **/
public interface CacheEntryExpiration {
   CacheEntryExpiration DEFAULT = new Impl();
   CacheEntryExpiration IMMORTAL = new Impl(Duration.ZERO, Duration.ZERO);

   static CacheEntryExpiration withLifespan(Duration lifespan) {
      return lifespan == null ? DEFAULT : new Impl(lifespan, null);
   }

   static CacheEntryExpiration withMaxIdle(Duration maxIdle) {
      return maxIdle == null ? DEFAULT : new Impl(null, maxIdle);
   }

   static CacheEntryExpiration withLifespanAndMaxIdle(Duration lifespan, Duration maxIdle) {
      return (lifespan == null && maxIdle == null) ? DEFAULT : new Impl(lifespan, maxIdle);
   }

   Optional<Duration> lifespan();

   Optional<Duration> maxIdle();

   boolean isImmortal();

   boolean isDefault();

   class Impl implements CacheEntryExpiration {
      private final Duration lifespan;
      private final Duration maxIdle;

      private Impl() {
         this.lifespan = null;
         this.maxIdle = null;
      }

      private Impl(Duration lifespan, Duration maxIdle) {
         this.lifespan = lifespan;
         this.maxIdle = maxIdle;
      }

      @Override
      public Optional<Duration> lifespan() {
         return Optional.ofNullable(lifespan);
      }

      @Override
      public Optional<Duration> maxIdle() {
         return Optional.ofNullable(maxIdle);
      }

      public Duration rawLifespan() {
         return lifespan;
      }

      public Duration rawMaxIdle() {
         return maxIdle;
      }

      @Override
      public boolean isImmortal() {
         return this == IMMORTAL;
      }

      @Override
      public boolean isDefault() {
         return this == DEFAULT;
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;
         Impl that = (Impl) o;
         return Objects.equals(lifespan, that.lifespan) && Objects.equals(maxIdle, that.maxIdle);
      }

      @Override
      public int hashCode() {
         return Objects.hash(lifespan, maxIdle);
      }

      @Override
      public String toString() {
         if (this == IMMORTAL) {
            return "Impl{IMMORTAL}";
         } else if (this == DEFAULT) {
            return "Impl{DEFAULT}";
         } else {
            return "Impl{" +
                  "lifespan=" + lifespan +
                  ", maxIdle=" + maxIdle +
                  '}';
         }
      }
   }
}
