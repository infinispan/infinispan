package org.infinispan.client.hotrod.impl;

import java.util.concurrent.TimeUnit;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.api.common.CacheWriteOptions;

final class CacheOptionsUtil {

   private CacheOptionsUtil() { }

   public static long lifespan(CacheWriteOptions options, long orElse, TimeUnit unit) {
      return lifespan(options.expiration(), orElse, unit);
   }

   public static long maxIdle(CacheWriteOptions options, long orElse, TimeUnit unit) {
      return maxIdle(options.expiration(), orElse, unit);
   }

   public static long lifespan(CacheEntryExpiration expiration, long orElse, TimeUnit unit) {
      return expiration.lifespan()
            .map(unit::convert)
            .orElse(orElse);

   }

   public static long maxIdle(CacheEntryExpiration expiration, long orElse, TimeUnit unit) {
      return expiration.maxIdle()
            .map(unit::convert)
            .orElse(orElse);

   }
}
