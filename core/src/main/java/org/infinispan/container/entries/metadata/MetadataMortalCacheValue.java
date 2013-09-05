package org.infinispan.container.entries.metadata;

import org.infinispan.metadata.Metadata;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.ImmortalCacheValue;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A mortal cache value, to correspond with
 * {@link org.infinispan.container.entries.metadata.MetadataMortalCacheEntry}
 *
 * @author Galder Zamarreño
 * @since 5.1
 */
public class MetadataMortalCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;
   long created;

   public MetadataMortalCacheValue(Object value, Metadata metadata, long created) {
      super(value);
      this.metadata = metadata;
      this.created = created;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MetadataMortalCacheEntry(key, value, metadata, created);
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
   public boolean isExpired() {
      return isExpired(System.currentTimeMillis());
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

   public static class Externalizer extends AbstractExternalizer<MetadataMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataMortalCacheValue mcv) throws IOException {
         output.writeObject(mcv.value);
         output.writeObject(mcv.metadata);
         UnsignedNumeric.writeUnsignedLong(output, mcv.created);
      }

      @Override
      public MetadataMortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataMortalCacheValue(v, metadata, created);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataMortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends MetadataMortalCacheValue>>asSet(MetadataMortalCacheValue.class);
      }
   }

}
