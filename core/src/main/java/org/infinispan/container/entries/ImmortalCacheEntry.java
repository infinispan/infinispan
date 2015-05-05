package org.infinispan.container.entries;

import static org.infinispan.commons.util.Util.toStr;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

/**
 * A cache entry that is immortal/cannot expire
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheEntry extends AbstractInternalCacheEntry {

   public Object value;

   public ImmortalCacheEntry(Object key, Object value) {
      super(key);
      this.value = value;
   }

   @Override
   public final boolean isExpired(long now) {
      return false;
   }

   @Override
   public final boolean isExpired() {
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
   public final void touch() {
      // no-op
   }

   @Override
   public void touch(long currentTimeMillis) {
      // no-op
   }

   @Override
   public final void reincarnate() {
      // no-op
   }

   @Override
   public void reincarnate(long now) {
      // no-op
   }

   @Override
   public InternalCacheValue toInternalCacheValue() {
      return new ImmortalCacheValue(value);
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
   public Metadata getMetadata() {
      return EmbeddedMetadata.DEFAULT;
   }

   @Override
   public void setMetadata(Metadata metadata) {
      throw new IllegalStateException(
            "Metadata cannot be set on immortal entries. They need to be recreated via the entry factory.");
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ImmortalCacheEntry that = (ImmortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      return result;
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
      }

      @Override
      public ImmortalCacheEntry readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object k = input.readObject();
         Object v = input.readObject();
         return new ImmortalCacheEntry(k, v);
      }

      @Override
      public Integer getId() {
         return Ids.IMMORTAL_ENTRY;
      }

      @Override
      public Set<Class<? extends ImmortalCacheEntry>> getTypeClasses() {
         return Util.<Class<? extends ImmortalCacheEntry>>asSet(ImmortalCacheEntry.class);
      }
   }

   @Override
   public String toString() {
      return "ImmortalCacheEntry{" +
            "key=" + toStr(key) +
            ", value=" + toStr(value) +
            "}";
   }

}
