package org.infinispan.container.entries;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheEntry extends AbstractInternalCacheEntry {
   private MortalCacheValue cacheValue;

   public Object getValue() {
      return cacheValue.value;
   }

   public Object setValue(Object value) {
      return cacheValue.setValue(value);
   }

   MortalCacheEntry(Object key, Object value, long lifespan) {
      this(key, value, lifespan, System.currentTimeMillis());
   }

   MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key);
      cacheValue = new MortalCacheValue(value, created, lifespan);
   }

   public final boolean isExpired() {
      return ExpiryHelper.isExpiredMortal(cacheValue.lifespan, cacheValue.created);
   }

   public final boolean canExpire() {
      return true;
   }

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle > -1) {
         TransientMortalCacheEntry tmce = new TransientMortalCacheEntry(key, cacheValue.value);
         tmce.setMaxIdle(maxIdle);
         return tmce;
      } else {
         return this;
      }
   }

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan < 0) {
         return new ImmortalCacheEntry(key, cacheValue.value);
      } else {
         cacheValue.lifespan = lifespan;
         return this;
      }
   }

   public final long getCreated() {
      return cacheValue.created;
   }

   public final long getLastUsed() {
      return -1;
   }

   public final long getLifespan() {
      return cacheValue.lifespan;
   }

   public final long getMaxIdle() {
      return -1;
   }

   public final long getExpiryTime() {
      return cacheValue.lifespan > -1 ? cacheValue.created + cacheValue.lifespan : -1;
   }

   public final void touch() {
      // no-op
   }

   public InternalCacheValue toInternalCacheValue() {
      return cacheValue;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MortalCacheEntry that = (MortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (cacheValue.value != null ? !cacheValue.value.equals(that.cacheValue.value) : that.cacheValue.value != null)
         return false;
      if (cacheValue.created != that.cacheValue.created) return false;
      return cacheValue.lifespan == that.cacheValue.lifespan;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (cacheValue.value != null ? cacheValue.value.hashCode() : 0);
      result = 31 * result + (int) (cacheValue.created ^ (cacheValue.created >>> 32));
      result = 31 * result + (int) (cacheValue.lifespan ^ (cacheValue.lifespan >>> 32));
      return result;
   }

   @Override
   public MortalCacheEntry clone() {
      MortalCacheEntry clone = (MortalCacheEntry) super.clone();
      clone.cacheValue = cacheValue.clone();
      return clone;
   }

}
