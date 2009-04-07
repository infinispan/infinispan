package org.infinispan.container.entries;

/**
 * A cache entry that is mortal.  I.e., has a lifespan.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheEntry extends AbstractInternalCacheEntry {
   long created;
   long lifespan = -1;

   MortalCacheEntry() {
      created = System.currentTimeMillis();
   }

   MortalCacheEntry(Object key, Object value, long lifespan) {
      super(key, value);
      created = System.currentTimeMillis();
      this.lifespan = lifespan;
   }

   MortalCacheEntry(Object key, Object value, long lifespan, long created) {
      super(key, value);
      this.created = created;
      this.lifespan = lifespan;
   }

   public final boolean isExpired() {
      return lifespan > -1 && System.currentTimeMillis() > created + lifespan;
   }

   public final boolean canExpire() {
      return true;
   }

   public InternalCacheEntry setMaxIdle(long maxIdle) {
      if (maxIdle > -1) {
         TransientMortalCacheEntry tmce = new TransientMortalCacheEntry(key, value);
         tmce.setMaxIdle(maxIdle);
         return tmce;
      } else {
         return this;
      }
   }

   public InternalCacheEntry setLifespan(long lifespan) {
      if (lifespan < 0) {
         return new ImmortalCacheEntry(key, value);
      } else {
         this.lifespan = lifespan;
         return this;
      }
   }

   public final long getCreated() {
      return created;
   }

   public final long getLastUsed() {
      return -1;
   }

   public final long getLifespan() {
      return lifespan;
   }

   public final long getMaxIdle() {
      return -1;
   }

   public final long getExpiryTime() {
      return lifespan > -1 ? created + lifespan : -1;
   }

   public final void touch() {
      // no-op
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MortalCacheEntry that = (MortalCacheEntry) o;

      if (key != null ? !key.equals(that.key) : that.key != null) return false;
      if (value != null ? !value.equals(that.value) : that.value != null) return false;
      if (created != that.created) return false;
      if (lifespan != that.lifespan) return false;

      return true;
   }

   @Override
   public int hashCode() {
      int result = key != null ? key.hashCode() : 0;
      result = 31 * result + (value != null ? value.hashCode() : 0);
      result = 31 * result + (int) (created ^ (created >>> 32));
      result = 31 * result + (int) (lifespan ^ (lifespan >>> 32));
      return result;
   }
}
