package org.infinispan.telemetry.jfr;

import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.InfinispanSpanContext;
import org.infinispan.telemetry.InfinispanTelemetry;

public class JfrTelemetry implements InfinispanTelemetry {

   private static final JfrTelemetry INSTANCE = new JfrTelemetry();

   public static JfrTelemetry getInstance() {
      return INSTANCE;
   }

   @Override
   public <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes) {
      var event = new JfrSpan<T>(operationName, attributes.cacheName().orElse("n/a"), String.valueOf(attributes.category()));
      event.begin();
      return event;
   }

   @Override
   public <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes, InfinispanSpanContext context) {
      var event = new JfrSpan<T>(operationName, attributes.cacheName().orElse("n/a"), String.valueOf(attributes.category()));
      event.begin();
      return event;
   }
}
