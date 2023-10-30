package org.infinispan.telemetry;

public interface InfinispanTelemetry {

   InfinispanSpan startTraceRequest(String operationName, InfinispanSpanAttributes attributes);

   InfinispanSpan startTraceRequest(String operationName, InfinispanSpanAttributes attributes, InfinispanSpanContext context);

}
