package org.infinispan.client.hotrod.impl;

import org.infinispan.client.hotrod.VersionedMetadata;

/**
 * @author Tristan Tarrant
 * @since 9.0
 */
public class VersionedMetadataImpl implements VersionedMetadata {
   private final long created;
   private final int lifespan;
   private final long lastUsed;
   private final int maxIdle;
   private final long version;


   public VersionedMetadataImpl(long created, int lifespan, long lastUsed, int maxIdle, long version) {
      this.created = created;
      this.lifespan = lifespan;
      this.lastUsed = lastUsed;
      this.maxIdle = maxIdle;
      this.version = version;
   }

   public long getCreated() {
      return created;
   }

   public int getLifespan() {
      return lifespan;
   }

   public long getLastUsed() {
      return lastUsed;
   }

   public int getMaxIdle() {
      return maxIdle;
   }

   public long getVersion() {
      return version;
   }
}
