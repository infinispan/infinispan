package org.infinispan.container.entries;

/**
 * An immortal cache value, to correspond with {@link org.infinispan.container.entries.ImmortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheValue implements InternalCacheValue {
   Object value;

   ImmortalCacheValue(Object value) {
      this.value = value;
   }

   public void setValue(Object value) {
      this.value = value;
   }

   public Object getValue() {
      return value;
   }

   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new ImmortalCacheEntry(key, value);
   }

   public boolean isExpired() {
      return false;
   }

   public boolean canExpire() {
      return false;
   }

   public long getCreated() {
      return -1;
   }

   public long getLastUsed() {
      return -1;
   }

   public long getLifespan() {
      return -1;
   }

   public long getMaxIdle() {
      return -1;
   }
}
