package org.horizon.container;

public class ExpirableCachedValue extends CachedValue {
   protected long createdTime;
   protected long expiryTime;

   protected ExpirableCachedValue() {
   }

   public ExpirableCachedValue(Object value, long createdTime, long expiryTime) {
      super(value);
      this.createdTime = createdTime;
      this.expiryTime = expiryTime;
   }

   public ExpirableCachedValue(Object value, long lifespan) {
      super(value);
      createdTime = getModifiedTime();
      setLifespan(lifespan);
   }

   public final boolean isExpired() {
      return expiryTime >= 0 && System.currentTimeMillis() > expiryTime;
   }

   public final long getCreatedTime() {
      return createdTime;
   }

   public final long getExpiryTime() {
      return expiryTime;
   }

   public final void setLifespan(long lifespan) {
      expiryTime = lifespan < 0 ? -1 : lifespan + createdTime;
   }

   @Override
   public final long getLifespan() {
      if (createdTime < 0 || expiryTime < 0) return -1;
      return expiryTime - createdTime;
   }
}