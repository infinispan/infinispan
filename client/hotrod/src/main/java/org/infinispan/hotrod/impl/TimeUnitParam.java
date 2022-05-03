package org.infinispan.hotrod.impl;

import java.time.Duration;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.infinispan.api.common.CacheEntryExpiration;

/**
 * Time unit representation for HotRod
 *
 * @since 14.0
 */
public class TimeUnitParam {

   private static final Map<TimeUnit, Byte> timeUnitToByte = new EnumMap<>(TimeUnit.class);

   static {
      timeUnitToByte.put(TimeUnit.SECONDS, (byte) 0);
      timeUnitToByte.put(TimeUnit.MILLISECONDS, (byte) 1);
      timeUnitToByte.put(TimeUnit.NANOSECONDS, (byte) 2);
      timeUnitToByte.put(TimeUnit.MICROSECONDS, (byte) 3);
      timeUnitToByte.put(TimeUnit.MINUTES, (byte) 4);
      timeUnitToByte.put(TimeUnit.HOURS, (byte) 5);
      timeUnitToByte.put(TimeUnit.DAYS, (byte) 6);
   }

   private static final byte TIME_UNIT_DEFAULT = (byte) 7;
   private static final byte TIME_UNIT_INFINITE = (byte) 8;

   private static byte encodeDuration(Duration duration) {
      return duration == Duration.ZERO ? TIME_UNIT_DEFAULT : duration == null ? TIME_UNIT_INFINITE : 0;
   }

   public static byte encodeTimeUnits(CacheEntryExpiration.Impl expiration) {
      byte encodedLifespan = encodeDuration(expiration.rawLifespan());
      byte encodedMaxIdle = encodeDuration(expiration.rawMaxIdle());
      return (byte) (encodedLifespan << 4 | encodedMaxIdle);
   }
}
