package org.infinispan.telemetry.impl;

import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.InfinispanSpanAttributes;
import org.infinispan.telemetry.InfinispanSpanContext;
import org.infinispan.telemetry.InfinispanTelemetry;

public class DisabledInfinispanTelemetry implements InfinispanTelemetry {

   @Override
   public <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes) {
      return DisabledInfinispanSpan.instance();
   }

   @Override
   public <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes, InfinispanSpanContext context) {
      return DisabledInfinispanSpan.instance();
   }
}
