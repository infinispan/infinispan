package org.infinispan.container.entries.metadata;

import static java.lang.Math.min;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.io.UnsignedNumeric;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.entries.ExpiryHelper;
import org.infinispan.container.entries.InternalCacheEntry;
import org.infinispan.container.entries.TransientMortalCacheValue;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A form of {@link TransientMortalCacheValue} that stores {@link Metadata}
 *
 * @author Manik Surtani
 * @since 5.1
 */
public class MetadataTransientMortalCacheValue extends MetadataMortalCacheValue implements MetadataAware {

   long lastUsed;

   public MetadataTransientMortalCacheValue(Object value, Metadata metadata, long created, long lastUsed) {
      this(value, null, metadata, created, lastUsed);
   }

   protected MetadataTransientMortalCacheValue(Object value, PrivateMetadata internalMetadata,
         Metadata metadata, long created, long lastUsed) {
      super(value, internalMetadata, metadata, created);
      this.lastUsed = lastUsed;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MetadataTransientMortalCacheEntry(key, value, internalMetadata, metadata, lastUsed, created);
   }

   @Override
   public long getMaxIdle() {
      return metadata.maxIdle();
   }

   @Override
   public long getLastUsed() {
      return lastUsed;
   }

   @Override
   public boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransientMortal(metadata.maxIdle(), lastUsed, metadata.lifespan(), created, now);
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
   }

   @Override
   public long getExpiryTime() {
      long lifespan = metadata.lifespan();
      long lset = lifespan > -1 ? created + lifespan : -1;
      long maxIdle = metadata.maxIdle();
      long muet = maxIdle > -1 ? lastUsed + maxIdle : -1;
      if (lset == -1) return muet;
      if (muet == -1) return lset;
      return min(lset, muet);
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", lastUsed=").append(lastUsed);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientMortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientMortalCacheValue value) throws IOException {
         output.writeObject(value.value);
         output.writeObject(value.internalMetadata);
         output.writeObject(value.metadata);
         UnsignedNumeric.writeUnsignedLong(output, value.created);
         UnsignedNumeric.writeUnsignedLong(output, value.lastUsed);
      }

      @Override
      public MetadataTransientMortalCacheValue readObject(ObjectInput input)
            throws IOException, ClassNotFoundException {
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long created = UnsignedNumeric.readUnsignedLong(input);
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientMortalCacheValue(value, internalMetadata, metadata, created, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_MORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataTransientMortalCacheValue>> getTypeClasses() {
         return Collections.singleton(MetadataTransientMortalCacheValue.class);
      }
   }

}
