package org.infinispan.container.entries;

import org.infinispan.marshall.AbstractExternalizer;
import org.infinispan.marshall.Ids;
import org.infinispan.util.Util;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * A cache entry that is immortal/cannot expire
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheEntry extends AbstractInternalCacheEntry {
   private ImmortalCacheValue cacheValue;

   ImmortalCacheEntry(Object key, Object value) {
      super(key);
      this.cacheValue = new ImmortalCacheValue(value);
   }

   public final boolean isExpired() {
      return false;
   }

   public final boolean canExpire() {
      return false;
   }

   public final long getCreated() {
      return -1;
   }

   public final long getLastUsed() {
      return -1;
   }

   public final long getLifespan() {
      return -1;
   }

   public final long getMaxIdle() {
      return -1;
   }

   public final long getExpiryTime() {
      return -1;
   }

   public final void touch() {
      // no-op
   }

   public final void reincarnate() {
      // no-op
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   public Object getValue() {
      return cacheValue.value;
   }

   public Object setValue(Object value) {
      return this.cacheValue.setValue(value);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ImmortalCacheEntry that = (ImmortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue != null ? !cacheValue.equals(that.cacheValue) : that.cacheValue != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue != null ? cacheValue.hashCode() : 0);
      return result;
   }

   @Override
   public ImmortalCacheEntry clone() {
      ImmortalCacheEntry clone = (ImmortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

   public static class Externalizer extends AbstractExternalizer<ImmortalCacheEntry> {
      @Override
      public void writeObject(ObjectOutput output, ImmortalCacheEntry ice) throws IOException {
         output.writeObject(ice.key);
         output.writeObject(ice.cacheValue.value);      
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
            "cacheValue=" + cacheValue +
            "} " + super.toString();
   }
}
