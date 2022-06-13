package org.infinispan.api.common;

import java.time.Duration;

/**
 * @since 14.0
 **/
public interface CacheWriteOptions extends CacheOptions {
   CacheWriteOptions DEFAULT = new Impl();

   static Builder writeOptions() {
      return new Builder();
   }

   static Builder writeOptions(CacheOptions options) {
      Builder builder = new Builder();
      options.timeout().ifPresent(builder::timeout);
      options.flags().ifPresent(builder::flags);
      return builder;
   }

   static Builder writeOptions(CacheWriteOptions options) {
      Builder builder = writeOptions((CacheOptions) options);
      options.expiration().lifespan().ifPresent(builder::lifespan);
      options.expiration().maxIdle().ifPresent(builder::maxIdle);
      return builder;
   }

   CacheEntryExpiration expiration();

   class Impl extends CacheOptions.Impl implements CacheWriteOptions {
      private final CacheEntryExpiration expiration;

      public Impl() {
         this(null, null, CacheEntryExpiration.DEFAULT);
      }

      Impl(Duration timeout, Flags<?, ?> flags, CacheEntryExpiration expiration) {
         super(timeout, flags);
         this.expiration = expiration != null ? expiration : CacheEntryExpiration.DEFAULT;
      }

      @Override
      public CacheEntryExpiration expiration() {
         return expiration;
      }
   }

   class Builder extends CacheOptions.Builder {
      private CacheEntryExpiration expiration = CacheEntryExpiration.DEFAULT;

      @Override
      public Builder timeout(Duration timeout) {
         super.timeout(timeout);
         return this;
      }

      @Override
      public Builder flags(Flags<?, ?> flags) {
         super.flags(flags);
         return this;
      }

      public Builder lifespan(Duration lifespan) {
         if (expiration.maxIdle().isPresent()) {
            return lifespanAndMaxIdle(lifespan, expiration.maxIdle().get());
         } else {
            this.expiration = CacheEntryExpiration.withLifespan(lifespan);
            return this;
         }
      }

      public Builder maxIdle(Duration maxIdle) {
         if (expiration.lifespan().isPresent()) {
            return lifespanAndMaxIdle(expiration.lifespan().get(), maxIdle);
         } else {
            this.expiration = CacheEntryExpiration.withMaxIdle(maxIdle);
            return this;
         }
      }

      public Builder lifespanAndMaxIdle(Duration lifespan, Duration maxIdle) {
         this.expiration = CacheEntryExpiration.withLifespanAndMaxIdle(lifespan, maxIdle);
         return this;
      }

      @Override
      public CacheWriteOptions build() {
         return new Impl(timeout, flags, expiration);
      }
   }
}
