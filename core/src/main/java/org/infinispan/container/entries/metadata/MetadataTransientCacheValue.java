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
import org.infinispan.container.entries.TransientCacheEntry;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A transient cache value, to correspond with {@link TransientCacheEntry} which is {@link MetadataAware}
 *
 * @author Galder Zamarreño
 * @since 5.3
 */
public class MetadataTransientCacheValue extends ImmortalCacheValue implements MetadataAware {

   Metadata metadata;
   long lastUsed;

   public MetadataTransientCacheValue(Object value, Metadata metadata, long lastUsed) {
      this(value, null, metadata, lastUsed);
   }

   protected MetadataTransientCacheValue(Object value, PrivateMetadata internalMetadata, Metadata metadata,
         long lastUsed) {
      super(value, internalMetadata);
      this.metadata = metadata;
      this.lastUsed = lastUsed;
   }

   @Override
   public InternalCacheEntry<?, ?> toInternalCacheEntry(Object key) {
      return new MetadataTransientCacheEntry(key, value, internalMetadata, metadata, lastUsed);
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
   public final boolean isExpired(long now) {
      return ExpiryHelper.isExpiredTransient(metadata.maxIdle(), lastUsed, now);
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public boolean isMaxIdleExpirable() {
      return true;
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
   public long getExpiryTime() {
      long maxIdle = metadata.maxIdle();
      return maxIdle > -1 ? lastUsed + maxIdle : -1;
   }

   @Override
   protected void appendFieldsToString(StringBuilder builder) {
      super.appendFieldsToString(builder);
      builder.append(", metadata=").append(metadata);
      builder.append(", lastUsed=").append(lastUsed);
   }

   public static class Externalizer extends AbstractExternalizer<MetadataTransientCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, MetadataTransientCacheValue tcv) throws IOException {
         output.writeObject(tcv.value);
         output.writeObject(tcv.internalMetadata);
         output.writeObject(tcv.metadata);
         UnsignedNumeric.writeUnsignedLong(output, tcv.lastUsed);
      }

      @Override
      public MetadataTransientCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         Metadata metadata = (Metadata) input.readObject();
         long lastUsed = UnsignedNumeric.readUnsignedLong(input);
         return new MetadataTransientCacheValue(value, internalMetadata, metadata, lastUsed);
      }

      @Override
      public Integer getId() {
         return Ids.METADATA_TRANSIENT_VALUE;
      }

      @Override
      public Set<Class<? extends MetadataTransientCacheValue>> getTypeClasses() {
         return Collections.singleton(MetadataTransientCacheValue.class);
      }
   }
}
