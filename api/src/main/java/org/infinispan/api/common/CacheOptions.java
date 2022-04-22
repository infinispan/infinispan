package org.infinispan.api.common;

import java.time.Duration;
import java.util.Optional;

/**
 * @since 14.0
 **/
public interface CacheOptions {
   CacheOptions DEFAULT = new Impl();

   static Builder options() {
      return new Builder();
   }

   Optional<Duration> timeout();

   Optional<Flags<?, ?>> flags();

   class Impl implements CacheOptions {
      private final Duration timeout;
      private final Flags<?, ?> flags;

      protected Impl() {
         this(null, null);
      }

      protected Impl(Duration timeout, Flags<?, ?> flags) {
         this.timeout = timeout;
         this.flags = flags;
      }

      @Override
      public Optional<Duration> timeout() {
         return Optional.ofNullable(timeout);
      }

      public Duration rawTimeout() {
         return timeout;
      }

      @Override
      public Optional<Flags<?, ?>> flags() {
         return Optional.ofNullable(flags);
      }

      public Flags<?, ?> rawFlags() {
         return flags;
      }
   }

   class Builder {
      protected Duration timeout;
      protected Flags<?, ?> flags;

      public Builder timeout(Duration timeout) {
         this.timeout = timeout;
         return this;
      }

      public Builder flags(Flags<?, ?> flags) {
         this.flags.addAll((Flags) flags);
         return this;
      }

      public CacheOptions build() {
         return new Impl(timeout, flags);
      }
   }
}
