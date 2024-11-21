package org.infinispan.client.hotrod.telemetry.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.impl.operations.AddClientListenerOperation;
import org.infinispan.client.hotrod.impl.operations.CacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.DelegatingCacheOperationsFactory;
import org.infinispan.client.hotrod.impl.operations.HotRodOperation;

public class TelemetryCacheOperationsFactory extends DelegatingCacheOperationsFactory {

   public TelemetryCacheOperationsFactory(CacheOperationsFactory delegate) {
      super(delegate);
   }

   @Override
   protected DelegatingCacheOperationsFactory newFactoryFor(CacheOperationsFactory factory) {
      return new TelemetryCacheOperationsFactory(factory);
   }

   @Override
   public <V> HotRodOperation<MetadataValue<V>> newRemoveOperation(Object key) {
      return new TelemetryOperation<>(super.newRemoveOperation(key));
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return new TelemetryOperation<>(super.newRemoveIfUnmodifiedOperation(key, version));
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      return new TelemetryOperation<>(super.newReplaceIfUnmodifiedOperation(key, value, lifespan, lifespanTimeUnit,
            maxIdle, maxIdleTimeUnit, version));
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new TelemetryOperation<>(super.newPutKeyValueOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new TelemetryOperation<>(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public <K, V> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return new TelemetryOperation<>(super.newReplaceOperation(key, valueBytes, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit));
   }

   @Override
   public HotRodOperation<Void> newClearOperation() {
      return new TelemetryOperation<>(super.newClearOperation());
   }

   @Override
   public HotRodOperation<Integer> newSizeOperation() {
      return new TelemetryOperation<>(super.newSizeOperation());
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener) {
      // TODO: look into this.. I doubt it made sense with includeCurrentState
      return super.newAddClientListenerOperation(listener);
   }

   @Override
   public AddClientListenerOperation newAddClientListenerOperation(Object listener, Object[] filterFactoryParams, Object[] converterFactoryParams) {
      // TODO: look into this.. I doubt it made sense with includeCurrentState
      return super.newAddClientListenerOperation(listener, filterFactoryParams, converterFactoryParams);
   }

   // TODO: putStream operations are not handled as well
}
