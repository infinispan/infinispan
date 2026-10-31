package org.infinispan.client.hotrod.near;

import java.util.Objects;

import org.infinispan.client.hotrod.MetadataValue;

public final class EpochMetadataValue<V> implements MetadataValue<V> {
   private final MetadataValue<V> delegate;
   private final int epoch;
   private final int segment;

   public EpochMetadataValue(MetadataValue<V> delegate, int epoch, int segment) {
      this.delegate = Objects.requireNonNull(delegate, "delegate");
      this.epoch = epoch;
      this.segment = segment;
   }

   public MetadataValue<V> getDelegate() {
      return delegate;
   }

   public int getEpoch() {
      return epoch;
   }

   public int getSegment() {
      return segment;
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
