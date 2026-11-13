package org.infinispan.client.hotrod.near;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerArray;

import org.infinispan.client.hotrod.MetadataValue;

final class EpochMetadataValue<V> implements MetadataValue<V> {
   private final MetadataValue<V> delegate;
   private final int epoch;
   private final int segment;
   private final AtomicIntegerArray epochs;

   EpochMetadataValue(MetadataValue<V> delegate, int epoch, int segment, AtomicIntegerArray epochs) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.epoch = epoch;
      this.segment = segment;
      this.epochs = epochs;
   }

   EpochMetadataValue<V> withValue(MetadataValue<V> value) {
      return new EpochMetadataValue<>(value, epoch, segment, epochs);
   }

   boolean isCurrent(AtomicIntegerArray currentEpochs) {
      // The segment was checked when this array was captured, and its length cannot change.
      return epochs == currentEpochs && epoch == epochs.get(segment);
   }

   MetadataValue<V> getDelegate() {
      return delegate;
   }

   @Override
   public long getCreated() {
      return delegate.getCreated();
   }

   @Override
   public int getLifespan() {
      return delegate.getLifespan();
   }

   @Override
   public long getLastUsed() {
      return delegate.getLastUsed();
   }

   @Override
   public int getMaxIdle() {
      return delegate.getMaxIdle();
   }

   @Override
   public long getVersion() {
      return delegate.getVersion();
   }

   @Override
   public V getValue() {
      return delegate.getValue();
   }

   @Override
   public String toString() {
      return "EpochMetadataValue[epoch=" + epoch + ", delegate=" + delegate + "]";
   }
}
