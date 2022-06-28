package org.infinispan.server.core.telemetry;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.propagation.TextMapGetter;

public interface TelemetryService {

   <T> Span requestStart(String operationName, TextMapGetter<T> textMapGetter, T carrier);

   void requestEnd(Object span);

   void recordException(Object span, Throwable throwable);

   class NoTelemetry implements TelemetryService {

      @Override
      public <T> Span requestStart(String operationName, TextMapGetter<T> textMapGetter, T carrier) {
         return null;
      }

      @Override
      public void requestEnd(Object span) {
         // do nothing
      }

      @Override
      public void recordException(Object span, Throwable throwable) {
         // do nothing
      }
   }
}
