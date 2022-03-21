package org.infinispan.server.core;

import io.opentelemetry.api.GlobalOpenTelemetry;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

public class RequestTracer {
   private static final Log log = LogFactory.getLog(RequestTracer.class);

   public static final String TRACING_ENABLED = System.getProperty("infinispan.tracing.enabled");

   private static Tracer tracer;

   static {
      if (TRACING_ENABLED != null && "true".equalsIgnoreCase(TRACING_ENABLED.trim())) {
         try {
            OpenTelemetry openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
                  .build()
                  .getOpenTelemetrySdk();

            tracer = openTelemetry.getTracer("org.infinispan.tracing", "1.0.0");
            log.tracerLoaded(tracer);
         } catch (Throwable e) {
            log.errorOnLoadingTracer();
         }
      } else {
         log.tracerDisabled();
      }
   }

   public static void start() {
      // start() method call is only needed to trigger the static initializer
   }

   public static void stop() {
      // theoretically OpenTelemetry should stop by itself (IIC)
      GlobalOpenTelemetry.resetForTest();
   }

   public static Span requestStart(String operationName) {
      if (tracer != null) {
         return tracer.spanBuilder(operationName).setSpanKind(SpanKind.CLIENT).startSpan();
      }
      return null;
   }

   public static void requestEnd(Span span) {
      if (tracer != null) {
         span.end();
      }
   }
}
