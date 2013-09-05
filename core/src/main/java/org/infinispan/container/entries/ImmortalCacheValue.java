package org.infinispan.container.entries;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.EmbeddedMetadata;
import org.infinispan.metadata.Metadata;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;

/**
 * An immortal cache value, to correspond with {@link org.infinispan.container.entries.ImmortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheValue implements InternalCacheValue, Cloneable {

   public Object value;

   public ImmortalCacheValue(Object value) {
      this.value = value;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new ImmortalCacheEntry(key, value);
   }

   public final Object setValue(Object value) {
      Object old = this.value;
      this.value = value;
      return old;
   }

   @Override
   public Object getValue() {
      return value;
   }

   @Override
   public boolean isExpired(long now) {
      return false;
   }

   @Override
   public boolean isExpired() {
      return false;
   }

   @Override
   public boolean canExpire() {
      return false;
   }

   @Override
   public long getCreated() {
      return -1;
   }

   @Override
   public long getLastUsed() {
      return -1;
   }

   @Override
   public long getLifespan() {
      return -1;
   }

   @Override
   public long getMaxIdle() {
      return -1;
   }

   @Override
   public long getExpiryTime() {
      return -1;
   }

   @Override
   public Metadata getMetadata() {
      return new EmbeddedMetadata.Builder().lifespan(getLifespan()).maxIdle(getMaxIdle()).build();
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof ImmortalCacheValue)) return false;

      ImmortalCacheValue that = (ImmortalCacheValue) o;

      if (value != null ? !value.equals(that.value) : that.value != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return value != null ? value.hashCode() : 0;
   }

   @Override
   public String toString() {
      return getClass().getSimpleName() + " {" +
            "value=" + value +
            '}';
   }

   @Override
   public ImmortalCacheValue clone() {
      try {
         return (ImmortalCacheValue) super.clone();
      } catch (CloneNotSupportedException e) {
         throw new RuntimeException("Should never happen", e);
      }
   }

   public static class Externalizer extends AbstractExternalizer<ImmortalCacheValue> {
      @Override
      public void writeObject(ObjectOutput output, ImmortalCacheValue icv) throws IOException {
         output.writeObject(icv.value);
      }

      @Override
      public ImmortalCacheValue readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Object v = input.readObject();
         return new ImmortalCacheValue(v);
      }

      @Override
      public Integer getId() {
         return Ids.IMMORTAL_VALUE;
      }

      @Override
      public Set<Class<? extends ImmortalCacheValue>> getTypeClasses() {
         return Util.<Class<? extends ImmortalCacheValue>>asSet(ImmortalCacheValue.class);
      }
   }
}
