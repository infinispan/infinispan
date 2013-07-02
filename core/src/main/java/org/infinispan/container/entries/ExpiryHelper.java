package org.infinispan.container.entries;

/**
 * Provide utility methods for dealing with expiration of cache entries.
 *
 * @author Manik Surtani
 * @author Sanne Grinovero
 * @since 4.0
 */
public class ExpiryHelper {

   public static boolean isExpiredMortal(long lifespan, long created, long now) {
      return lifespan > -1 && created > -1 && now > created + lifespan;
   }

   public static boolean isExpiredTransient(long maxIdle, long lastUsed, long now) {
      return maxIdle > -1 && lastUsed > -1 && now > maxIdle + lastUsed;
   }

   public static boolean isExpiredTransientMortal(long maxIdle, long lastUsed, long lifespan, long created, long now) {
      return isExpiredTransient(maxIdle, lastUsed, now) || isExpiredMortal(lifespan, created, now);
   }

}
