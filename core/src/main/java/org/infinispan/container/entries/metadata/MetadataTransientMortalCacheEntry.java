package org.infinispan.container.entries.metadata;

import static java.lang.Math.min;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.entries.AbstractInternalCacheEntry;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A form of {@link org.infinispan.container.entries.TransientMortalCacheEntry}
 * that is {@link org.infinispan.container.entries.versioned.Versioned}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class MetadataTransientMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   Object value;
   Metadata metadata;
   long created;
   long lastUsed;

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long now) {
      this(key, value, metadata, now, now);
   }

   public MetadataTransientMortalCacheEntry(Object key, Object value, Metadata metadata, long lastUsed, long created) {
      super(key);
      this.value = value;
      this.metadata = metadata;
      this.lastUsed = lastUsed;
      this.created = created;
   }

   @Override
   public Object getValue() {
      return value;
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
   public boolean isExpired() {
      return isExpired(System.currentTimeMillis());
   }

   @Override
   public final long getExpiryTime() {
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = metadata.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   public InternalCacheValue toInternalCacheValue(boolean includeInvocationRecords) {
      return new MetadataTransientMortalCacheValue(value, includeInvocationRecords ? metadata : metadata.builder().noInvocations().build(), created, lastUsed);
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public final void touch() {
      lastUsed = System.currentTimeMillis();
   }

   @Override
   public final void touch(long currentTimeMillis) {
      lastUsed = currentTimeMillis;
   }

   @Override
   public final void reincarnate() {
      reincarnate(System.currentTimeMillis());
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
   public Object setValue(Object value) {
      return this.value = value;
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
   protected boolean hasMetadata() {
      return true;
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientMortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.metadata);
         UnsignedNumeric.writeUnsignedLong(output, ice.created);
         UnsignedNumeric.writeUnsignedLong(output, ice.lastUsed);
      }

      @Override
      public MetadataTransientMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientMortalCacheEntry(k, v, metadata, lastUsed, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataTransientMortalCacheEntry>> getTypeClasses() {
         return Util.asSet(MetadataTransientMortalCacheEntry.class);
      }
   }
}
