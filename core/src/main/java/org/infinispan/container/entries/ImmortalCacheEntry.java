package org.infinispan.container.entries;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;
import org.infinispan.metadata.impl.PrivateMetadata;

/**
 * A cache entry that is immortal/cannot expire
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheEntry extends AbstractInternalCacheEntry {

   public ImmortalCacheEntry(Object key, Object value) {
      this(key, value, null);
   }

   protected ImmortalCacheEntry(Object key, Object value, PrivateMetadata internalMetadata) {
      super(key, value, internalMetadata);
   }

   @Override
   public final boolean isExpired(long now) {
      return false;
   }

   @Override
   public final boolean canExpire() {
      return false;
   }

   @Override
   public final long getCreated() {
      return -1;
   }

   @Override
   public final long getLastUsed() {
      return -1;
   }

   @Override
   public final long getLifespan() {
      return -1;
   }

   @Override
   public final long getMaxIdle() {
      return -1;
   }

   @Override
   public final long getExpiryTime() {
      return -1;
   }

   @Override
   public void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      // no-op
   }

   @Override
   public InternalCacheValue<?> toInternalCacheValue() {
      return new ImmortalCacheValue(value, internalMetadata);
   }

   @Override
   public Metadata getMetadata() {
      return EmbeddedMetadata.EMPTY;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on immortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public ImmortalCacheEntry clone() {
      return (ImmortalCacheEntry) super.clone();
   }

   public static class Externalizer extends AbstractExternalizer<ImmortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, ImmortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.value);
         output.writeObject(ice.internalMetadata);
      }

      @Override
      public ImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object key = input.readObject();
         Object value = input.readObject();
         PrivateMetadata internalMetadata = (PrivateMetadata) input.readObject();
         return new ImmortalCacheEntry(key, value, internalMetadata);
      }

      @Override
      public Integer getId() {
         return Ids.IMMORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends ImmortalCacheEntry>> getTypeClasses() {
         return Collections.singleton(ImmortalCacheEntry.class);
      }
   }

}
