package org.infinispan.telemetry.impl;

import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.SafeAutoClosable;

public class DisabledInfinispanSpan implements InfinispanSpan {

   private static final InfinispanSpan INSTANCE = new DisabledInfinispanSpan();

   public static InfinispanSpan instance() {
      return INSTANCE;
   }

   private DisabledInfinispanSpan() {
   }

   @Override
   public SafeAutoClosable makeCurrent() {
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
