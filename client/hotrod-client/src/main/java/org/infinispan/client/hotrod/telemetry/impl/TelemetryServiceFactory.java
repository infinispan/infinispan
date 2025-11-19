package org.infinispan.client.hotrod.telemetry.impl;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

public class TelemetryServiceFactory {

   private static final Log log = LogFactory.getLog(TelemetryService.class);

   public static final TelemetryService INSTANCE;

   static {
      INSTANCE = telemetryService();
   }

   public static TelemetryService telemetryService(boolean propagationEnabled) {
      if (INSTANCE == null) {
         // in case of null, we've already logged either
         // noOpenTelemetryAPI or errorCreatingPropagationContext
         return NoOpTelemetryService.INSTANCE;
      }

      if (propagationEnabled) {
         log.openTelemetryPropagationEnabled();
         return INSTANCE;
      }

      // We log this only if the telemetry service is available,
      // but the user disable to propagate the tracing:
      log.openTelemetryPropagationDisabled();
      return NoOpTelemetryService.INSTANCE;
   }

   private static TelemetryService telemetryService() {
      try {
         Class.forName("io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator", false,
               TelemetryService.class.getClassLoader());
         return TelemetryServiceImpl.INSTANCE;
      } catch (ClassNotFoundException e) {
         log.noOpenTelemetryAPI();
      } catch (Throwable e) {
         log.errorCreatingPropagationContext(e);
      }
      return null;
   }
}
