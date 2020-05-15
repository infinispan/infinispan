package org.infinispan.container.entries.metadata;

import static java.lang.Math.min;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.container.entries.TransientMortalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A form of {@link TransientMortalCacheEntry} that stores {@link Metadata}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class MetadataTransientMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   Metadata metadata;
   long created;
   long lastUsed;

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long now) {
      this(key, value, metadata, now, now);
   }

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long lastUsed, long created) {
      this(key, value, null, metadata, lastUsed, created);
   }

   protected MetadataTransientMortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata,
         Metadata metadata, long lastUsed, long created) {
      super(key, value, internalMetadata);
      this.metadata = metadata;
      this.lastUsed = lastUsed;
      this.created = created;
   }

   @Override
   public long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public long getCreated() {
      return created;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(
            metadata.maxIdle(), lastUsed, metadata.lifespan(), created, now);
   }

   @Override
   public boolean canExpireMaxIdle() {
      return true;
   }

   @Override
   public final long getExpiryTime() {
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = metadata.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) {
         return muet;
      }
      if (muet == -1) {
         return lset;
      }
      return min(lset, muet);
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new MetadataTransientMortalCacheValue(value, internalMetadata, metadata, created, lastUsed);
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      lastUsed = currentTimeMillis;
   }

   @Override
   public void reincarnate(long now) {
      created = now;
   }

   @Override
   public long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public Metadata getMetadata() {
      return metadata;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      this.metadata = metadata;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", created=").append(created);
      builder.append(", lastUsed=").append(lastUsed);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientMortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.internalMetadata);
         output.writeObject(ice.metadata);
         UnsignedNumeric.writeUnsignedLong(output, ice.created);
         UnsignedNumeric.writeUnsignedLong(output, ice.lastUsed);
      }

      @Override
      public MetadataTransientMortalCacheEntry readObject(ObjectInput input)
            throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientMortalCacheEntry(key, value, internalMetadata, metadata, lastUsed, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataTransientMortalCacheEntry>> getTypeClasses() {
         return Collections.singleton(MetadataTransientMortalCacheEntry.class);
      }
   }
}
