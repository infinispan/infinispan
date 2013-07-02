package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.MetadataValue;

/**
 * MetadataValueImpl.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class MetadataValueImpl<V> extends VersionedValueImpl<V> implements MetadataValue<V> {

   private final long created;
   private final int lifespan;
   private final long lastUsed;
   private final int maxIdle;

   public MetadataValueImpl(long created, int lifespan, long lastUsed, int maxIdle, long version, V value) {
      super(version, value);
      this.created = created;
      this.lifespan = lifespan;
      this.lastUsed = lastUsed;
      this.maxIdle = maxIdle;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public int getLifespan() {
      return lifespan;
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public int getMaxIdle() {
      return maxIdle;
   }

   @Override
   public String toString() {
      return "MetadataValueImpl [created=" + created + ", lifespan=" + lifespan + ", lastUsed=" + lastUsed + ", maxIdle=" + maxIdle + ", getVersion()=" + getVersion()
            + ", getValue()=" + getValue() + "]";
   }

}
