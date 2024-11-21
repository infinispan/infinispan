package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

public interface MultimapOperationsFactory {
   <K, V> HotRodOperation<Collection<V>> newGetKeyMultimapOperation(K key, boolean supportsDuplicates);

   <K, V> HotRodOperation<MetadataCollection<V>> newGetKeyWithMetadataMultimapOperation(K key, boolean supportsDuplicates);

   <K, V> HotRodOperation<Void> newPutKeyValueOperation(K key, V value, long lifespan,
                                                        TimeUnit lifespanTimeUnit, long maxIdle,
                                                        TimeUnit maxIdleTimeUnit, boolean supportsDuplicates);

   <K> HotRodOperation<Boolean> newRemoveKeyOperation(K key, boolean supportsDuplicates);

   <K, V> HotRodOperation<Boolean> newRemoveEntryOperation(K key, V value, boolean supportsDuplicates);

   <K, V> HotRodOperation<Boolean> newContainsEntryOperation(K key, V value, boolean supportsDuplicates);

   <K> HotRodOperation<Boolean> newContainsKeyOperation(K key, boolean supportsDuplicates);

   HotRodOperation<Boolean> newContainsValueOperation(byte[] value, boolean supportsDuplicates);

   HotRodOperation<Long> newSizeOperation(boolean supportsDuplicates);
}
