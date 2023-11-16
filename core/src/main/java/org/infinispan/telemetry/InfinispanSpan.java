package org.infinispan.telemetry;

import java.util.function.BiConsumer;

public interface InfinispanSpan<T> extends BiConsumer<T, Throwable> {

   SafeAutoClosable makeCurrent();

   void complete();

   void recordException(Throwable throwable);

   @Override
   default void accept(T ignored, Throwable throwable) {
      if (throwable != null) {
         recordException(throwable);
      }
      complete();
   }
}
