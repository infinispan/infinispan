package org.infinispan.hotrod.telemetry.impl;

import org.infinispan.hotrod.impl.logging.Log;
import org.infinispan.hotrod.impl.logging.LogFactory;
import org.infinispan.hotrod.impl.protocol.HeaderParams;

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
