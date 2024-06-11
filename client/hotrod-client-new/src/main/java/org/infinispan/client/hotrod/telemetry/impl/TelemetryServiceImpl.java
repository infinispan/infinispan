package org.infinispan.client.hotrod.telemetry.impl;

import java.util.function.Function;

import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;

public class TelemetryServiceImpl implements TelemetryService {
   public static TelemetryServiceImpl INSTANCE = new TelemetryServiceImpl();
   private TelemetryServiceImpl() { }
   @Override
   public <K, V> Function<InternalRemoteCache<K, V>, CacheOperationsFactory> wrapWithTelemetry(
         Function<InternalRemoteCache<K, V>, CacheOperationsFactory> function) {
      return rc -> new TelemetryCacheOperationsFactory(function.apply(rc));
   }
}
