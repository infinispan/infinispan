package org.infinispan.client.hotrod.telemetry.impl;

import org.infinispan.client.hotrod.impl.protocol.HeaderParams;
import org.infinispan.client.hotrod.logging.Log;
import org.infinispan.client.hotrod.logging.LogFactory;

public interface TelemetryService {

   Log log = LogFactory.getLog(TelemetryService.class, Log.class);

   TelemetryService INSTANCE = createInstance();

   static TelemetryService createInstance() {
      try {
         Class.forName("io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator", false,
               TelemetryService.class.getClassLoader());
      } catch (ClassNotFoundException e) {
         log.noOpenTelemetryAPI();
         return null;
      }

      try {
         return new TelemetryServiceImpl();
      } catch (Throwable e) {
         log.errorCreatingPropagationContext(e);
         return null;
      }
   }

   void injectSpanContext(HeaderParams header);

}
