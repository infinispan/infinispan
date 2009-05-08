package org.infinispan.container.entries;

/**
 * A cache entry that is immortal/cannot expire
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class ImmortalCacheEntry extends AbstractInternalCacheEntry {

   ImmortalCacheEntry(Object key, Object value) {
      super(key, value);
   }

   public final boolean isExpired() {
      return false;
   }

   public final boolean canExpire() {
      return false;
   }

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle > -1) {
         TransientCacheEntry tce = new TransientCacheEntry();
         tce.setMaxIdle(maxIdle);
         tce.key = key;
         tce.value = value;
         return tce;
      } else {
         return this;
      }
   }

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan > -1) {
         MortalCacheEntry mce = new MortalCacheEntry();
         mce.setLifespan(lifespan);
         mce.key = key;
         mce.value = value;
         return mce;
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
      return new ImmortalCacheValue(value);
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
}
