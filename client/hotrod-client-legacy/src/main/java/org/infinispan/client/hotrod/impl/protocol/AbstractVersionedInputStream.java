package org.infinispan.client.hotrod.impl.protocol;

import java.io.IOException;
import java.io.InputStream;

import org.infinispan.client.hotrod.VersionedMetadata;

import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public abstract class AbstractVersionedInputStream extends InputStream implements VersionedMetadata {
   protected final VersionedMetadata versionedMetadata;
   protected final Runnable afterClose;

   public AbstractVersionedInputStream(VersionedMetadata versionedMetadata, Runnable afterClose) {
      this.versionedMetadata = versionedMetadata;
      this.afterClose = afterClose;
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

   @Override
   public void close() throws IOException {
      super.close();
      if (afterClose != null) {
         afterClose.run();
      }
   }
}
