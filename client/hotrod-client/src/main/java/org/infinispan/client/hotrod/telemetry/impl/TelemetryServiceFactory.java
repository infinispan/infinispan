package org.infinispan.client.hotrod.telemetry.impl;

import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

public class TelemetryServiceFactory {

   private static final Log log = LogFactory.getLog(TelemetryService.class, Log.class);

   public static final TelemetryServiceFactory INSTANCE = new TelemetryServiceFactory();

   private final TelemetryService telemetryService;

   private TelemetryServiceFactory() {
      telemetryService = telemetryService();
   }

   public TelemetryService telemetryService(boolean propagationEnabled) {
      if (telemetryService == null) {
         // in case of null, we've already logged either
         // noOpenTelemetryAPI or errorCreatingPropagationContext
         return null;
      }

      if (propagationEnabled) {
         log.openTelemetryPropagationEnabled();
         return telemetryService;
      }

      // We log this only if the telemetry service is available,
      // but the user disable to propagate the tracing:
      log.openTelemetryPropagationDisabled();
      return null;
   }

   private TelemetryService telemetryService() {
      try {
         Class.forName("io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator", false,
               TelemetryService.class.getClassLoader());
      } catch (ClassNotFoundException e) {
         log.noOpenTelemetryAPI();
         return null;
      }

      try {
         TelemetryServiceImpl service = new TelemetryServiceImpl();
         return service;
      } catch (Throwable e) {
         log.errorCreatingPropagationContext(e);
         return null;
      }
   }
}
