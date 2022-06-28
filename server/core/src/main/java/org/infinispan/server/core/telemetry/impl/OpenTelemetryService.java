package org.infinispan.server.core.telemetry.impl;

import org.infinispan.server.core.telemetry.TelemetryService;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.propagation.TextMapGetter;

public class OpenTelemetryService implements TelemetryService {

   private static final String INFINISPAN_SERVER_TRACING_NAME = "org.infinispan.server.tracing";
   private static final String INFINISPAN_SERVER_TRACING_VERSION = "1.0.0";

   private final OpenTelemetry openTelemetry;
   private final Tracer tracer;

   public OpenTelemetryService(OpenTelemetry openTelemetry) {
      this.openTelemetry = openTelemetry;
      this.tracer = openTelemetry.getTracer(INFINISPAN_SERVER_TRACING_NAME, INFINISPAN_SERVER_TRACING_VERSION);
   }

   @Override
   public <T> Span requestStart(String operationName, TextMapGetter<T> textMapGetter, T carrier) {
      Context extractedContext = openTelemetry.getPropagators().getTextMapPropagator()
            .extract(Context.current(), carrier, textMapGetter);

      Span span = tracer.spanBuilder(operationName)
            .setSpanKind(SpanKind.SERVER)
            .setParent(extractedContext).startSpan();
      return span;
   }

   @Override
   public void requestEnd(Object span) {
      ((Span) span).end();
   }

   @Override
   public void recordException(Object span, Throwable throwable) {
      Span casted = (Span) span;
      casted.setStatus(StatusCode.ERROR, "Error during the cache request processing");
      casted.recordException(throwable);
   }
}
