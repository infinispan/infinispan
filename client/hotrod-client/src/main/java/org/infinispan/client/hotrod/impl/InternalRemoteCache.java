package org.infinispan.client.hotrod.impl;

import java.util.Map;
import java.util.concurrent.CompletionStage;

import javax.management.ObjectName;

import org.infinispan.client.hotrod.DataFormat;
import org.infinispan.client.hotrod.Flag;
import org.infinispan.client.hotrod.RemoteCache;
import org.infinispan.client.hotrod.VersionedValue;
import org.infinispan.client.hotrod.configuration.Configuration;
import org.infinispan.client.hotrod.impl.operations.OperationsFactory;
import org.infinispan.client.hotrod.impl.operations.PingResponse;
import org.infinispan.commons.marshall.Marshaller;
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

   @Override
   InternalRemoteCache<K, V> withFlags(Flag... flags);

   @Override
   <T, U> InternalRemoteCache<T, U> withDataFormat(DataFormat dataFormat);

   boolean hasForceReturnFlag();

   void resolveStorage(boolean objectStorage);

   @Override
   ClientStatistics clientStatistics();

   void init(Marshaller marshaller, OperationsFactory operationsFactory, Configuration configuration,
         ObjectName jmxParent);

   void init(Marshaller marshaller, OperationsFactory operationsFactory, Configuration configuration);

   OperationsFactory getOperationsFactory();

   boolean isObjectStorage();

   K keyAsObjectIfNeeded(Object key);

   byte[] keyToBytes(Object o);

   CompletionStage<PingResponse> ping();
}
