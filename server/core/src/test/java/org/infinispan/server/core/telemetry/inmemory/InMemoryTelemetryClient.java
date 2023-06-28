package org.infinispan.server.core.telemetry.inmemory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.context.Scope;
import io.opentelemetry.sdk.trace.data.SpanData;

public class InMemoryTelemetryClient {

   private final InMemoryTelemetryService telemetryService;
   private final Tracer tracer;
   private final InMemorySpanExporter inMemorySpanExporter;

   public InMemoryTelemetryClient() {
      telemetryService = InMemoryTelemetryService.instance();
      inMemorySpanExporter = telemetryService.spanExporter();
      tracer = telemetryService.openTelemetry().getTracer("org.infinispan.hotrod.client.test", "1.0.0");
   }

   @SuppressWarnings("unused")
   public void withinClientSideSpan(String spanName, Runnable operations) {
      Span span = tracer.spanBuilder(spanName).setSpanKind(SpanKind.CLIENT).startSpan();
      // put the span into the current Context
      try (Scope scope = span.makeCurrent()) {
         operations.run();
      } catch (Throwable throwable) {
         span.setStatus(StatusCode.ERROR, "Something bad happened!");
         span.recordException(throwable);
         throw throwable;
      } finally {
         span.end(); // Cannot set a span after this call
      }
   }

   public List<SpanData> finishedSpanItems() {
      return inMemorySpanExporter.getFinishedSpanItems();
   }

   public void reset() {
      telemetryService.reset();
   }

   public InMemoryTelemetryService telemetryService() {
      return telemetryService;
   }

   public static Map<String, List<SpanData>> aggregateByName(List<SpanData> spans) {
      return spans.stream().collect(Collectors.groupingBy(SpanData::getName, Collectors.toList()));
   }
}
