package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.Collection;

import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.impl.operations.RoutingObjectOperation;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

public class RoutingMultimapOperationsFactory extends DelegatingMultimapOperationsFactory {

   public RoutingMultimapOperationsFactory(MultimapOperationsFactory delegate) {
      super(delegate);
   }

   @Override
   public <K, V> HotRodOperation<Collection<V>> newGetKeyMultimapOperation(K key, boolean supportsDuplicates) {
      return new RoutingObjectOperation<>(super.newGetKeyMultimapOperation(key, supportsDuplicates), key);
   }

   @Override
   public <K, V> HotRodOperation<MetadataCollection<V>> newGetKeyWithMetadataMultimapOperation(K key, boolean supportsDuplicates) {
      return new RoutingObjectOperation<>(super.newGetKeyWithMetadataMultimapOperation(key, supportsDuplicates), key);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newContainsEntryOperation(K key, V value, boolean supportsDuplicates) {
      return new RoutingObjectOperation<>(super.newContainsEntryOperation(key, value, supportsDuplicates), key);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key, boolean supportsDuplicates) {
      return new RoutingObjectOperation<>(super.newContainsKeyOperation(key, supportsDuplicates), key);
   }

   @Override
   public <K> HotRodOperation<Boolean> newRemoveKeyOperation(K key, boolean supportsDuplicates) {
      return new RoutingObjectOperation<>(super.newRemoveKeyOperation(key, supportsDuplicates), key);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newRemoveEntryOperation(K key, V value, boolean supportsDuplicates) {
      return new RoutingObjectOperation<>(super.newRemoveEntryOperation(key, value, supportsDuplicates), key);
   }
}
