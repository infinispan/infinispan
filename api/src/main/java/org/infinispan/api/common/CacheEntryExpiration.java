package org.infinispan.api.common;

import java.time.Duration;
import java.util.Optional;

/**
 * @since 14.0
 **/
public interface CacheEntryExpiration {
   CacheEntryExpiration IMMORTAL = new Impl();

   static CacheEntryExpiration withLifespan(Duration lifespan) {
      return lifespan == null ? IMMORTAL : new Impl(lifespan, null);
   }

   static CacheEntryExpiration withMaxIdle(Duration maxIdle) {
      return maxIdle == null ? IMMORTAL : new Impl(null, maxIdle);
   }

   static CacheEntryExpiration withLifespanAndMaxIdle(Duration lifespan, Duration maxIdle) {
      return (lifespan == null && maxIdle == null) ? IMMORTAL : new Impl(lifespan, maxIdle);
   }

   Optional<Duration> lifespan();

   Optional<Duration> maxIdle();

   boolean isImmortal();

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
   }
}
