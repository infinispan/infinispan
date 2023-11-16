package org.infinispan.telemetry;

public interface InfinispanTelemetry {

   <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes);

   <T> InfinispanSpan<T> startTraceRequest(String operationName, InfinispanSpanAttributes attributes, InfinispanSpanContext context);

}
