package org.infinispan.server.core.telemetry;

import static org.infinispan.util.logging.Log.CONTAINER;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

import org.infinispan.commons.logging.Log;
import org.infinispan.configuration.global.GlobalTracingConfiguration;
import org.infinispan.configuration.global.TracingExporterProtocol;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;
import org.infinispan.server.core.telemetry.inmemory.InMemoryTelemetryService;
import org.infinispan.telemetry.InfinispanTelemetry;
import org.infinispan.telemetry.impl.DisabledInfinispanTelemetry;
import org.jboss.logging.Logger;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = InfinispanTelemetry.class)
public class TelemetryServiceFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   public static final String IN_MEMORY_COLLECTOR_ENDPOINT = "file://in-memory-local-process";

   private static final Log log = Logger.getMessageLogger(Log.class, "org.infinispan.SERVER");

   @Override
   public Object construct(String componentName) {
      if (!componentName.equals(InfinispanTelemetry.class.getName())) {
         throw CONTAINER.factoryCannotConstructComponent(componentName);
      }

      GlobalTracingConfiguration tracing = globalConfiguration.tracing();
      if (!tracing.enabled()) {
         return new DisabledInfinispanTelemetry();
      }

      if (IN_MEMORY_COLLECTOR_ENDPOINT.equals(tracing.collectorEndpoint())) {
         return new OpenTelemetryService(InMemoryTelemetryService.instance().openTelemetry());
      }

      try {
         OpenTelemetry openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
               .addPropertiesSupplier(() -> autoConfigureOpenTelemetryProperties(tracing))
               .build()
               .getOpenTelemetrySdk();

         log.telemetryLoaded(openTelemetry);
         return new OpenTelemetryService(openTelemetry);
      } catch (Throwable e) {
         log.errorOnLoadingTelemetry(e);
         return new DisabledInfinispanTelemetry();
      }
   }

   private Map<String, String> autoConfigureOpenTelemetryProperties(GlobalTracingConfiguration tracing) {
      HashMap<String, String> result = new HashMap<>();

      result.put("otel.metrics.exporter", "none");
      result.put("otel.logs.exporter", "none");
      result.put("otel.exporter.otlp.protocol", "http/protobuf");
      result.put("otel.service.name", tracing.serviceName());

      switch (tracing.exporterProtocol()) {
         case OTLP:
            result.put("otel.traces.exporter", TracingExporterProtocol.OTLP.name().toLowerCase());
            result.put("otel.exporter.otlp.endpoint", tracing.collectorEndpoint());
            break;
         case JAEGER:
            result.put("otel.traces.exporter", TracingExporterProtocol.JAEGER.name().toLowerCase());
            result.put("otel.exporter.jaeger.endpoint", tracing.collectorEndpoint());
            break;
         case ZIPKIN:
            result.put("otel.traces.exporter", TracingExporterProtocol.ZIPKIN.name().toLowerCase());
            result.put("otel.exporter.zipkin.endpoint", tracing.collectorEndpoint());
            break;
         case PROMETHEUS:
            try {
               URL url = new URL(tracing.collectorEndpoint());
               result.put("otel.traces.exporter", TracingExporterProtocol.PROMETHEUS.name().toLowerCase());
               result.put("otel.exporter.prometheus.host", url.getHost());
               result.put("otel.exporter.prometheus.port", url.getPort() + "");
            } catch (MalformedURLException e) {
               throw log.errorOnParsingPrometheusURLForTracing(e);
            }
            break;
      }
      return result;
   }
}
