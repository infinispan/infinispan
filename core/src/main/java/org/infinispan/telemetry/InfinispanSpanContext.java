package org.infinispan.telemetry;

public interface InfinispanSpanContext {

   Iterable<String> keys();

   String getKey(String key);

}
