package org.infinispan.client.hotrod.near;

import org.infinispan.client.hotrod.MetadataValue;

public final class EpochMetadataValue<V> implements MetadataValue<V> {
   private final MetadataValue<V> delegate;
   private final int epoch;
   private final int segment;

   public EpochMetadataValue(MetadataValue<V> delegate, int epoch, int segment) {
      this.delegate = delegate;
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
      return delegate != null ? delegate.getCreated() : -1;
   }

   @Override
   public int getLifespan() {
      return delegate != null ? delegate.getLifespan() : -1;
   }

   @Override
   public long getLastUsed() {
      return delegate != null ? delegate.getLastUsed() : -1;
   }

   @Override
   public int getMaxIdle() {
      return delegate != null ? delegate.getMaxIdle() : -1;
   }

   @Override
   public long getVersion() {
      return delegate != null ? delegate.getVersion() : -1;
   }

   @Override
   public V getValue() {
      return delegate != null ? delegate.getValue() : null;
   }

   @Override
   public String toString() {
      return "EpochMetadataValue[epoch=" + epoch + ", delegate=" + delegate + "]";
   }
}
