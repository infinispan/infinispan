package org.infinispan.telemetry.impl;

import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.SafeAutoClosable;

public class DisabledInfinispanSpan<T> implements InfinispanSpan<T>, SafeAutoClosable {

   private static final InfinispanSpan<?> INSTANCE = new DisabledInfinispanSpan<>();

   public static <E> InfinispanSpan<E> instance() {
      //noinspection unchecked
      return (InfinispanSpan<E>) INSTANCE;
   }

   private DisabledInfinispanSpan() {
   }

   @Override
   public SafeAutoClosable makeCurrent() {
      return this;
   }

   @Override
   public void complete() {
      // no-op
   }

   @Override
   public void recordException(Throwable throwable) {
      // no-op
   }

   @Override
   public void close() {
      //no-op
   }
}
