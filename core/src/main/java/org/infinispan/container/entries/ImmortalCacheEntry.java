package org.infinispan.container.entries;

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

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle > -1) {
         return new TransientCacheEntry(key, cacheValue.value, maxIdle);
      } else {
         return this;
      }
   }

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan > -1) {
         return new MortalCacheEntry(key, cacheValue.value, lifespan);
      } else {
         return this;
      }
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
}
