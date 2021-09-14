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

   /**
    * Returns the most recent (i.e. smallest number) that is not negative or if both are negative it returns a negative
    * number
    *
    * @param firstTime  one of the times
    * @param secondTime one of the times
    * @return the lowest time of each, that is not negative unless both are negative
    */
   public static long mostRecentExpirationTime(long firstTime, long secondTime) {
      if (firstTime < 0) {
         return secondTime;
      } else if (secondTime < 0) {
         return firstTime;
      }
      return Math.min(firstTime, secondTime);
   }
}
