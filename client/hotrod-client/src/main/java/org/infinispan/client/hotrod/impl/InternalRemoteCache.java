package org.infinispan.client.hotrod.impl;

import java.net.SocketAddress;
import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.management.ObjectName;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.MetadataValue;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.client.hotrod.impl.operations.RetryAwareCompletionStage;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.CloseableIterator;
import org.infinispan.commons.util.IntSet;

public interface InternalRemoteCache<K, V> extends RemoteCache<K, V> {

   CloseableIterator<K> keyIterator(IntSet segments);

   CloseableIterator<Map.Entry<K, V>> entryIterator(IntSet segments);

   default boolean removeEntry(Map.Entry<K, V> entry) {
      return removeEntry(entry.getKey(), entry.getValue());
   }

   default boolean removeEntry(K key, V value) {
      VersionedValue<V> versionedValue = getWithMetadata(key);
      return versionedValue != null && value.equals(versionedValue.getValue()) &&
            removeWithVersion(key, versionedValue.getVersion());
   }

   RetryAwareCompletionStage<MetadataValue<V>> getWithMetadataAsync(K key, SocketAddress preferredAddres);

   @Override
   InternalRemoteCache<K, V> withFlags(Flag... flags);

   @Override
   <T, U> InternalRemoteCache<T, U> withDataFormat(DataFormat dataFormat);

   boolean hasForceReturnFlag();

   void resolveStorage(boolean objectStorage);

   default void resolveStorage(MediaType key, MediaType value, boolean objectStorage) {
      resolveStorage(objectStorage);
   }

   @Override
   ClientStatistics clientStatistics();

   void init(OperationsFactory operationsFactory, Configuration configuration,
             ObjectName jmxParent);

   void init(OperationsFactory operationsFactory, Configuration configuration);

   OperationsFactory getOperationsFactory();

   boolean isObjectStorage();

   byte[] keyToBytes(Object o);

   CompletionStage<PingResponse> ping();

   /**
    * Add a client listener to handle near cache with bloom filter optimization
    * The listener object must be annotated with @{@link org.infinispan.client.hotrod.annotation.ClientListener} annotation.
    */
   SocketAddress addNearCacheListener(Object listener, int bloomBits);

   /**
    * Sends the current bloom filter to the listener node where a near cache listener is installed. If this
    * cache does not have near caching this will return an already completed stage.
    * @return stage that when complete the filter was sent to the listener node
    */
   CompletionStage<Void> updateBloomFilter();
}
