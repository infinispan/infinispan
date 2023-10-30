package org.infinispan.server.core.telemetry;

import java.util.Objects;

import org.infinispan.telemetry.InfinispanSpan;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;

public class OpenTelemetrySpan implements InfinispanSpan {

   private final Span span;

   public OpenTelemetrySpan(Span span) {
      this.span = Objects.requireNonNull(span);
   }

   @Override
   public AutoCloseable makeCurrent() {
      return span.makeCurrent();
   }

   @Override
   public void complete() {
      span.end();
   }

   @Override
   public void recordException(Throwable throwable) {
      span.setStatus(StatusCode.ERROR, "Error during the cache request processing");
      span.recordException(throwable);
   }
}
