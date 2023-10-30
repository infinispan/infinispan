package org.infinispan.telemetry.impl;

import org.infinispan.telemetry.InfinispanSpan;

public class DisabledInfinispanSpan implements InfinispanSpan {

   private static final InfinispanSpan INSTANCE = new DisabledInfinispanSpan();

   public static InfinispanSpan instance() {
      return INSTANCE;
   }

   private DisabledInfinispanSpan() {
   }

   @Override
   public AutoCloseable makeCurrent() {
      return () -> {
         // nothing to close
      };
   }

   @Override
   public void complete() {
      // no-op
   }

   @Override
   public void recordException(Throwable throwable) {
      // no-op
   }
}
