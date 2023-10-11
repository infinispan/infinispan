package org.infinispan.server.core.telemetry.inmemory;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.SdkTracerProviderBuilder;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.samplers.Sampler;

public class InMemoryTelemetryService {

   private static InMemoryTelemetryService singleton;

   private final InMemorySpanExporter spanExporter = InMemorySpanExporter.create();
   private final SdkTracerProvider tracerProvider;
   private final OpenTelemetry openTelemetry;

   private InMemoryTelemetryService() {
      GlobalOpenTelemetry.resetForTest();

      SpanProcessor spanProcessor = SimpleSpanProcessor.create(spanExporter);
      SdkTracerProviderBuilder builder = SdkTracerProvider.builder()
            .setSampler(Sampler.alwaysOn())
            .addSpanProcessor(spanProcessor);
      tracerProvider = builder.build();

      openTelemetry = OpenTelemetrySdk.builder()
            .setTracerProvider(tracerProvider)
            .setPropagators(ContextPropagators.create(W3CTraceContextPropagator.getInstance()))
            .buildAndRegisterGlobal();
   }

   public static synchronized InMemoryTelemetryService instance() {
      if (singleton == null) {
         singleton = new InMemoryTelemetryService();
      }
      return singleton;
   }

   public void reset() {
     tracerProvider.forceFlush();
     spanExporter.reset();
   }

   public OpenTelemetry openTelemetry() {
      return openTelemetry;
   }

   public InMemorySpanExporter spanExporter() {
      return spanExporter;
   }
}
