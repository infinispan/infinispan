package org.infinispan.rest.tracing;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Scope;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

public class OpenTelemetryClient {

   private final SdkTracerProvider tracerProvider;
   private final OpenTelemetry openTelemetry;
   private final Tracer tracer;

   public OpenTelemetryClient(SpanExporter spanExporter) {
      // we usually use a batch processor,
      // but this is a test
      SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);
      SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
            .addSpanProcessor(spanProcessor);

      tracerProvider = builder.build();
      // JUnit5 TestNG engine creates two instances of test class, so we must explicitly reset GlobalOpenTelemetry to
      // prevent an IllegalStateException on the second invocation.
      GlobalOpenTelemetry.resetForTest();
      openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
      tracer = openTelemetry.getTracer("org.infinispan.hotrod.client.test", "1.0.0");
   }

   public void shutdown() {
      tracerProvider.shutdown();
      GlobalOpenTelemetry.resetForTest();
   }

   public OpenTelemetry openTelemetry() {
      return openTelemetry;
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
}
