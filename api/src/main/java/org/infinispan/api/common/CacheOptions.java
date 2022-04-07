package org.infinispan.api.common;

import java.time.Duration;
import java.util.EnumSet;
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

   Optional<EnumSet<?>> flags();

   interface Flag<E extends Enum<E>> {
      EnumSet<E> apply(EnumSet<E> flags);
   }

   class Impl implements CacheOptions {
      private final Duration timeout;
      private final EnumSet<?> flags;

      protected Impl() {
         this(null, null);
      }

      protected Impl(Duration timeout, EnumSet<?> flags) {
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
      public Optional<EnumSet<?>> flags() {
         return Optional.ofNullable(flags);
      }

      public EnumSet<?> rawFlags() {
         return flags;
      }
   }

   class Builder {
      protected Duration timeout;
      protected EnumSet<?> flags;

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

      public CacheOptions build() {
         return new Impl(timeout, flags);
      }
   }
}
