package org.infinispan.server.core.telemetry;

import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.factories.AbstractComponentFactory;
import org.infinispan.factories.AutoInstantiableFactory;
import org.infinispan.factories.annotations.DefaultFactoryFor;
import org.infinispan.factories.scopes.Scope;
import org.infinispan.factories.scopes.Scopes;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.trace.Tracer;
import io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk;

@Scope(Scopes.GLOBAL)
@DefaultFactoryFor(classes = OpenTelemetry.class)
public class OpenTelemetryFactory extends AbstractComponentFactory implements AutoInstantiableFactory {

   private static final Log log = LogFactory.getLog(OpenTelemetryFactory.class);

   private static final String TRACING_ENABLED = System.getProperty("infinispan.tracing.enabled");

   @Override
   public Object construct(String name) {
      if (TRACING_ENABLED == null || !"true".equalsIgnoreCase(TRACING_ENABLED.trim())) {
         log.telemetryDisabled();
         return null;
      }

      try {
         OpenTelemetry openTelemetry = AutoConfiguredOpenTelemetrySdk.builder()
               .build()
               .getOpenTelemetrySdk();

         log.telemetryLoaded(openTelemetry);
         return openTelemetry;
      } catch (Throwable e) {
         log.errorOnLoadingTelemetry();
         return null;
      }
   }

   public static Tracer getTracer(OpenTelemetry openTelemetry) {
      return (openTelemetry == null) ? null : openTelemetry.getTracer("org.infinispan.server.tracing", "1.0.0");
   }
}
