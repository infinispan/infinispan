package org.infinispan.rest.tracing;

import org.infinispan.rest.framework.RestRequest;
import org.infinispan.server.core.telemetry.TelemetryService;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.propagation.TextMapGetter;

public class RestTelemetryService {

   private static final TextMapGetter<RestRequest> REST_REQUEST_TEXT_MAP_GETTER =
         new TextMapGetter<>() {
            @Override
            public String get(RestRequest carrier, String key) {
               return carrier.header(key);
            }

            @Override
            public Iterable<String> keys(RestRequest carrier) {
               return carrier.headersKeys();
            }
         };

   private final TelemetryService telemetryService;

   public RestTelemetryService(TelemetryService telemetryService) {
      this.telemetryService = telemetryService;
   }

   public Span requestStart(String operationName, RestRequest request) {
      return telemetryService.requestStart(operationName, REST_REQUEST_TEXT_MAP_GETTER, request);
   }

   public void requestEnd(Object span) {
      telemetryService.requestEnd(span);
   }

   public void recordException(Object span, Throwable throwable) {
      telemetryService.recordException(span, throwable);
   }
}
