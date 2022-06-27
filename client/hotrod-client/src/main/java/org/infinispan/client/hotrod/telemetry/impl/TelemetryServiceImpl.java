package org.infinispan.client.hotrod.telemetry.impl;

import java.nio.charset.StandardCharsets;

import org.infinispan.client.hotrod.impl.protocol.HeaderParams;

import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.Context;

public class TelemetryServiceImpl implements TelemetryService {

   private final W3CTraceContextPropagator propagator;

   public TelemetryServiceImpl() {
      propagator = W3CTraceContextPropagator.getInstance();
   }

   public void injectSpanContext(HeaderParams header) {
      // Inject the request with the *current* Context, which contains client current Span if exists.
      propagator.inject(Context.current(), header,
            (carrier, paramKey, paramValue) ->
                  carrier.otherParam(paramKey, paramValue.getBytes(StandardCharsets.UTF_8))
      );
   }
}
