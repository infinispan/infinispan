package org.infinispan.client.hotrod.impl.protocol;

import java.io.InputStream;

import org.infinispan.client.hotrod.VersionedMetadata;

public abstract class AbstractVersionedInputStream extends InputStream implements VersionedMetadata {
   protected final VersionedMetadata versionedMetadata;

   public AbstractVersionedInputStream(VersionedMetadata versionedMetadata) {
      this.versionedMetadata = versionedMetadata;
   }

   @Override
   public long getVersion() {
      return versionedMetadata.getVersion();
   }

   @Override
   public long getCreated() {
      return versionedMetadata.getCreated();
   }

   @Override
   public int getLifespan() {
      return versionedMetadata.getLifespan();
   }

   @Override
   public long getLastUsed() {
      return versionedMetadata.getLastUsed();
   }

   @Override
   public int getMaxIdle() {
      return versionedMetadata.getMaxIdle();
   }
}
