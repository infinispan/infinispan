package org.infinispan.server.hotrod.tracing;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.infinispan.server.core.telemetry.TelemetryService;

import io.opentelemetry.api.trace.Span;
import io.opentelemetry.context.propagation.TextMapGetter;

public class HotRodTelemetryService {

   private static final TextMapGetter<Map<String, byte[]>> MAP_TEXT_MAP_GETTER =
         new TextMapGetter<>() {
            @Override
            public String get(Map<String, byte[]> carrier, String key) {
               byte[] bytes = carrier.get(key);
               if (bytes == null) {
                  return null;
               }

               return new String(bytes, StandardCharsets.UTF_8);
            }

            @Override
            public Iterable<String> keys(Map<String, byte[]> carrier) {
               return carrier.keySet();
            }
         };

   private final TelemetryService telemetryService;

   public HotRodTelemetryService(TelemetryService telemetryService) {
      this.telemetryService = telemetryService;
   }

   public Span requestStart(String operationName, Map<String, byte[]> otherParams) {
      return telemetryService.requestStart(operationName, MAP_TEXT_MAP_GETTER, otherParams);
   }

   public void requestEnd(Object span) {
      telemetryService.requestEnd(span);
   }

   public void recordException(Object span, Throwable throwable) {
      telemetryService.recordException(span, throwable);
   }
}
