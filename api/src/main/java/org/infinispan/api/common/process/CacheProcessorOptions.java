package org.infinispan.api.common.process;

import java.time.Duration;

import org.infinispan.api.common.CacheOptions;
import org.infinispan.api.common.Flags;

/**
 * @since 14.0
 **/
public interface CacheProcessorOptions extends CacheOptions {
   CacheProcessorOptions DEFAULT = new Impl();

   static Builder processorOptions() {
      return new Builder();
   }

   Object[] arguments();

   class Impl extends CacheOptions.Impl implements CacheProcessorOptions {
      final Object[] arguments;

      public Impl() {
         this(null, null, null);
      }

      private Impl(Duration timeout, Flags<?, ?> flags, Object[] arguments) {
         super(timeout, flags);
         this.arguments = arguments;
      }

      @Override
      public Object[] arguments() {
         return arguments;
      }
   }

   class Builder extends CacheOptions.Builder {
      private Object[] arguments;

      public Builder arguments(Object... arguments) {
         this.arguments = arguments;
         return this;
      }

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

      @Override
      public CacheProcessorOptions build() {
         return new Impl(timeout, flags, arguments);
      }
   }
}
