package org.infinispan.container.entries.metadata;

import java.io.IOException;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.marshall.UserObjectInput;
import org.infinispan.commons.marshall.UserObjectOutput;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A cache entry that is transient, i.e., it can be considered expired after
 * a period of not being used, and {@link org.infinispan.container.entries.metadata.MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataTransientCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected Object value;
   protected Metadata metadata;
   protected long lastUsed;

   public MetadataTransientCacheEntry(Object key, Object value, Metadata metadata, long lastUsed) {
      super(key);
      this.value = value;
      this.metadata = metadata;
      this.lastUsed = lastUsed;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public Object setValue(Object value) {
      return this.value = value;
   }

   @Override
   public final void touch() {
      touch(System.currentTimeMillis());
   }

   @Override
   public final void touch(long currentTimeMillis) {
      lastUsed = currentTimeMillis;
   }


   @Override
   public final void reincarnate() {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      //no-op
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(metadata.maxIdle(), lastUsed, now);
   }

   @Override
   public boolean isExpired() {
      return isExpired(System.currentTimeMillis());
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public final long getLastUsed() {
      return lastUsed;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      long maxIdle = metadata.maxIdle();
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   public final long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new MetadataTransientCacheValue(value, metadata, lastUsed);
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientCacheEntry> {
      @Override
      public void writeObject(UserObjectOutput output, MetadataTransientCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.metadata);
         UnsignedNumeric.writeUnsignedLong(output, ice.lastUsed);
      }

      @Override
      public MetadataTransientCacheEntry readObject(UserObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientCacheEntry(k, v, metadata, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataTransientCacheEntry>> getTypeClasses() {
         return Util.asSet(MetadataTransientCacheEntry.class);
      }
   }
}
