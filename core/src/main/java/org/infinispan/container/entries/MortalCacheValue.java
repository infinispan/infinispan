package org.infinispan.container.entries;

/**
 * A mortal cache value, to correspond with {@link org.infinispan.container.entries.MortalCacheEntry}
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class MortalCacheValue extends ImmortalCacheValue {
   long created;
   long lifespan = -1;

   MortalCacheValue(Object value, long created, long lifespan) {
      super(value);
      this.created = created;
      this.lifespan = lifespan;
   }

   @Override
   public long getCreated() {
      return created;
   }

   public void setCreated(long created) {
      this.created = created;
   }

   @Override
   public long getLifespan() {
      return lifespan;
   }

   public void setLifespan(long lifespan) {
      this.lifespan = lifespan;
   }

   @Override
   public boolean isExpired() {
      return ExpiryHelper.isExpiredMortal(lifespan, created);
   }

   @Override
   public boolean canExpire() {
      return true;
   }

   @Override
   public InternalCacheEntry toInternalCacheEntry(Object key) {
      return new MortalCacheEntry(key, value, lifespan, created);
   }
}
