package org.infinispan.client.hotrod.impl.multimap.operations;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.operations.HotRodOperation;
import org.infinispan.client.hotrod.multimap.MetadataCollection;

public class DelegatingMultimapOperationsFactory implements MultimapOperationsFactory {
   private final MultimapOperationsFactory delegate;

   public DelegatingMultimapOperationsFactory(MultimapOperationsFactory delegate) {
      this.delegate = delegate;
   }

   @Override
   public <K, V> HotRodOperation<Collection<V>> newGetKeyMultimapOperation(K key, boolean supportsDuplicates) {
      return delegate.newGetKeyMultimapOperation(key, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<MetadataCollection<V>> newGetKeyWithMetadataMultimapOperation(K key, boolean supportsDuplicates) {
      return delegate.newGetKeyWithMetadataMultimapOperation(key, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Void> newPutKeyValueOperation(K key, V value, long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit, boolean supportsDuplicates) {
      return delegate.newPutKeyValueOperation(key, value, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit, supportsDuplicates);
   }

   @Override
   public <K> HotRodOperation<Boolean> newRemoveKeyOperation(K key, boolean supportsDuplicates) {
      return delegate.newRemoveKeyOperation(key, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newRemoveEntryOperation(K key, V value, boolean supportsDuplicates) {
      return delegate.newRemoveEntryOperation(key, value, supportsDuplicates);
   }

   @Override
   public <K, V> HotRodOperation<Boolean> newContainsEntryOperation(K key, V value, boolean supportsDuplicates) {
      return delegate.newContainsEntryOperation(key, value, supportsDuplicates);
   }

   @Override
   public <K> HotRodOperation<Boolean> newContainsKeyOperation(K key, boolean supportsDuplicates) {
      return delegate.newContainsKeyOperation(key, supportsDuplicates);
   }

   @Override
   public HotRodOperation<Boolean> newContainsValueOperation(byte[] value, boolean supportsDuplicates) {
      return delegate.newContainsValueOperation(value, supportsDuplicates);
   }

   @Override
   public HotRodOperation<Long> newSizeOperation(boolean supportsDuplicates) {
      return delegate.newSizeOperation(supportsDuplicates);
   }
}
