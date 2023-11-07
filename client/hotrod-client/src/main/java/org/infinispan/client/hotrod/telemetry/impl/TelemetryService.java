package org.infinispan.client.hotrod.telemetry.impl;

import org.infinispan.client.hotrod.impl.protocol.HeaderParams;

public interface TelemetryService {

   void injectSpanContext(HeaderParams header);

}
