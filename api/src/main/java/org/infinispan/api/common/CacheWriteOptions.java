package org.infinispan.api.common;

import java.time.Duration;
import java.util.EnumSet;

/**
 * @since 14.0
 **/
public interface CacheWriteOptions extends CacheOptions {
   CacheWriteOptions DEFAULT = new Impl();

   static Builder writeOptions() {
      return new Builder();
   }

   CacheEntryExpiration expiration();

   class Impl extends CacheOptions.Impl implements CacheWriteOptions {
      private final CacheEntryExpiration expiration;

      public Impl() {
         this(null, null, CacheEntryExpiration.IMMORTAL);
      }

      Impl(Duration timeout, EnumSet<?> flags, CacheEntryExpiration expiration) {
         super(timeout, flags);
         this.expiration = expiration != null ? expiration : CacheEntryExpiration.IMMORTAL;
      }

      @Override
      public CacheEntryExpiration expiration() {
         return expiration;
      }
   }

   class Builder {
      private Duration timeout;
      private EnumSet<?> flags;
      private CacheEntryExpiration expiration;

      public Builder timeout(Duration timeout) {
         this.timeout = timeout;
         return this;
      }

      public Builder flags(Flag flag) {
         flags = flag.apply(flags);
         return this;
      }

      public Builder flags(Flag... flags) {
         for (Flag flag : flags) {
            this.flags = flag.apply(this.flags);
         }
         return this;
      }

      public Builder lifespan(Duration lifespan) {
         this.expiration = CacheEntryExpiration.withLifespan(lifespan);
         return this;
      }

      public Builder maxIdle(Duration maxIdle) {
         this.expiration = CacheEntryExpiration.withMaxIdle(maxIdle);
         return this;
      }

      public Builder lifespanAndMaxIdle(Duration lifespan, Duration maxIdle) {
         this.expiration = CacheEntryExpiration.withLifespanAndMaxIdle(lifespan, maxIdle);
         return this;
      }

      public CacheWriteOptions build() {
         return new Impl(timeout, flags, expiration);
      }
   }
}
