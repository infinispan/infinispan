package org.infinispan.container.entries.metadata;

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
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A cache entry that is mortal and is {@link MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataMortalCacheEntry extends AbstractInternalCacheEntry implements MetadataAware {

   protected Metadata metadata;
   protected long created;

   public MetadataMortalCacheEntry(Object key, Object value, Metadata metadata, long created) {
      this(key, value, null, metadata, created);
   }

   protected MetadataMortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata,
         Metadata metadata, long created) {
      super(key, value, internalMetadata);
      this.metadata = metadata;
      this.created = created;
   }

   @Override
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(metadata.lifespan(), created, now);
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   public final long getCreated() {
      return created;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      long lifespan = metadata.lifespan();
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public final void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      this.created = now;
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new MetadataMortalCacheValue(value, internalMetadata, metadata, created);
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
   }

   public static class Externalizer extends AbstractExternalizer<MetadataMortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, MetadataMortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.internalMetadata);
         output.writeObject(ice.metadata);
         UnsignedNumeric.writeUnsignedLong(output, ice.created);
      }

      @Override
      public MetadataMortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataMortalCacheEntry(key, value, internalMetadata, metadata, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_MORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends MetadataMortalCacheEntry>> getTypeClasses() {
         return Collections.singleton(MetadataMortalCacheEntry.class);
      }
   }
}
