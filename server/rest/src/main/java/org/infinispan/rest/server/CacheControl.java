package org.infinispan.rest.server;

public class CacheControl {

   private static final String NO_CACHE_HEADER_VALUE = "no-cache";
   private static final String MAX_AGE_HEADER_VALUE = "max-age";

   private static final CacheControl NO_CACHE = new CacheControl(NO_CACHE_HEADER_VALUE);

   private final String cacheControl;

   private CacheControl(String cacheControl) {
      this.cacheControl = cacheControl;
   }

   public static CacheControl noCache() {
      return NO_CACHE;
   }

   public static CacheControl maxAge(int timeInSeconds) {
      return new CacheControl(MAX_AGE_HEADER_VALUE + "=" + timeInSeconds);
   }

   @Override
   public String toString() {
      return cacheControl;
   }
}
