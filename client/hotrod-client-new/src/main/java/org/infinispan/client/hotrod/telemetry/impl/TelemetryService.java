package org.infinispan.client.hotrod.telemetry.impl;

import java.util.function.Function;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;

public interface TelemetryService {

   <K, V> Function<InternalRemoteCache<K, V>, CacheOperationsFactory> wrapWithTelemetry(Function<InternalRemoteCache<K, V>, CacheOperationsFactory> function);

}
