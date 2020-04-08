package org.infinispan.container.entries.metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A mortal cache value, to correspond with {@link MetadataMortalCacheEntry}
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class MetadataMortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;
   long created;

   public MetadataMortalCacheValue(Object value, Metadata metadata, long created) {
      this(value, null, metadata, created);
   }

   protected MetadataMortalCacheValue(Object value, PrivateMetadata internalMetadata, Metadata metadata, long created) {
      super(value, internalMetadata);
      this.metadata = metadata;
      this.created = created;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MetadataMortalCacheEntry(key, value, internalMetadata, metadata, created);
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
   public final long getCreated() {
      return created;
   }

   @Override
   public final long getLifespan() {
      return metadata.lifespan();
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredMortal(metadata.lifespan(), created, now);
   }

   @Override
   public long getExpiryTime() {
      long lifespan = metadata.lifespan();
      return lifespan > -1 ? created + lifespan : -1;
   }

   @Override
   public final boolean canExpire() {
      return true;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", created=").append(created);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataMortalCacheValue mcv) throws IOException {
         output.writeObject(mcv.value);
         output.writeObject(mcv.internalMetadata);
         output.writeObject(mcv.metadata);
         UnsignedNumeric.writeUnsignedLong(output, mcv.created);
      }

      @Override
      public MetadataMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataMortalCacheValue(value, internalMetadata, metadata, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataMortalCacheValue>> getTypeClasses() {
         return Collections.singleton(MetadataMortalCacheValue.class);
      }
   }

}
