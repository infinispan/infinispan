package org.infinispan.server.core.telemetry;

import java.util.Objects;

import org.infinispan.telemetry.InfinispanSpan;
import org.infinispan.telemetry.SafeAutoClosable;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.api.trace.StatusCode;
import io.opentelemetry.context.Scope;

public class OpenTelemetrySpan<T> implements InfinispanSpan<T> {

   private final Span span;

   public OpenTelemetrySpan(Span span) {
      this.span = Objects.requireNonNull(span);
   }

   @Override
   public SafeAutoClosable makeCurrent() {
      //noinspection resource
      Scope scope = span.makeCurrent();
      return scope::close;
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
