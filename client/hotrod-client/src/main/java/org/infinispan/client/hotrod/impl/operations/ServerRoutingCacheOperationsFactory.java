package org.infinispan.client.hotrod.impl.operations;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.impl.InternalRemoteCache;
import org.infinispan.client.hotrod.impl.VersionedOperationResponse;
import org.infinispan.client.hotrod.marshall.MediaTypeMarshaller;

import io.netty.channel.Channel;

public class ServerRoutingCacheOperationsFactory extends DelegatingCacheOperationsFactory {
   public ServerRoutingCacheOperationsFactory(CacheOperationsFactory delegate) {
      super(delegate);
   }

   private <V> HotRodOperation<V> wrapAsNecessary(HotRodOperation<V> op, Object key) {
      InternalRemoteCache<?, ?> irc = getRemoteCache();
      DataFormat format = irc.getDataFormat();
      MediaTypeMarshaller serverFormat = format.server();
      if (serverFormat == null || serverFormat.equals(format.client())) {
         return op;
      }
      return new RoutingObjectOperation<>(op, serverFormat.keyToBytes(key));
   }

   @Override
   public <V> HotRodOperation<V> newGetOperation(Object key) {
      return wrapAsNecessary(super.newGetOperation(key), key);
   }

   @Override
   public <K, V> HotRodOperation<GetWithMetadataOperation.GetWithMetadataResult<V>> newGetWithMetadataOperation(K key, Channel preferredChannel) {
      return wrapAsNecessary(super.newGetWithMetadataOperation(key, preferredChannel), key);
   }

   @Override
   public <V> HotRodOperation<MetadataValue<V>> newRemoveOperation(Object key) {
      return wrapAsNecessary(super.newRemoveOperation(key), key);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return wrapAsNecessary(super.newPutKeyValueOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return wrapAsNecessary(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   public <K, V> HotRodOperation<MetadataValue<V>> newPutIfAbsentOperation(K key, V value, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit, Flag... flags) {
      return wrapAsNecessary(super.newPutIfAbsentOperation(key, value, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit, flags), key);
   }

   @Override
   public <K, V> HotRodOperation<V> newReplaceOperation(K key, V valueBytes, long lifespan, TimeUnit lifespanUnit, long maxIdleTime, TimeUnit maxIdleTimeUnit) {
      return wrapAsNecessary(super.newReplaceOperation(key, valueBytes, lifespan, lifespanUnit, maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newReplaceIfUnmodifiedOperation(K key, V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, long version) {
      return wrapAsNecessary(super.newReplaceIfUnmodifiedOperation(key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, version), key);
   }

   @Override
   public <K, V> HotRodOperation<VersionedOperationResponse<V>> newRemoveIfUnmodifiedOperation(K key, long version) {
      return wrapAsNecessary(super.newRemoveIfUnmodifiedOperation(key, version), key);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key) {
      return wrapAsNecessary(super.newContainsKeyOperation(key), key);
   }

   @Override
   public <T> HotRodOperation<T> executeOperation(String taskName, Map<String, byte[]> marshalledParams, Object key) {
      return wrapAsNecessary(super.executeOperation(taskName, marshalledParams, key), key);
   }

   @Override
   public HotRodOperation<GetStreamStartResponse> newGetStreamStartOperation(Object key, int batchSize) {
      return wrapAsNecessary(super.newGetStreamStartOperation(key, batchSize), key);
   }

   @Override
   public HotRodOperation<PutStreamResponse> newPutStreamStartOperation(Object key, long version, long lifespan,
                                                                        TimeUnit lifespanUnit, long maxIdleTime,
                                                                        TimeUnit maxIdleTimeUnit) {
      return wrapAsNecessary(super.newPutStreamStartOperation(key, version, lifespan, lifespanUnit,
            maxIdleTime, maxIdleTimeUnit), key);
   }

   @Override
   protected DelegatingCacheOperationsFactory newFactoryFor(CacheOperationsFactory factory) {
      return new ServerRoutingCacheOperationsFactory(factory);
   }
}
