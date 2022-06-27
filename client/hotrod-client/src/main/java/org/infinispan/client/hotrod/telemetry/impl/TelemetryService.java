package org.infinispan.client.hotrod.telemetry.impl;

import org.infinispan.client.hotrod.impl.protocol.HeaderParams;

public interface TelemetryService {

   /**
    * Try to create a {@link TelemetryService} instance.
    *
    * @throws ClassCastException if the OpenTelemetry API module is not in the classpath
    * @return a {@link TelemetryService} instance
    */
   static TelemetryService create() {
      return new TelemetryServiceImpl();
   }

   void injectSpanContext(HeaderParams header);

}
