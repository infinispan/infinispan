package org.infinispan.client.hotrod.impl;

import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Time unit representation for HotRod
 *
 * @author gustavonalle
 * @since 8.0
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

   private static byte encodeDuration(long duration, TimeUnit timeUnit) {
      return duration == 0 ? TIME_UNIT_DEFAULT : duration < 0 ? TIME_UNIT_INFINITE : timeUnitToByte.get(timeUnit);
   }

   public static byte encodeTimeUnits(long lifespan, TimeUnit lifespanTimeUnit, long maxIdle, TimeUnit maxIdleTimeUnit) {
      byte encodedLifespan = encodeDuration(lifespan, lifespanTimeUnit);
      byte encodedMaxIdle = encodeDuration(maxIdle, maxIdleTimeUnit);
      return (byte) (encodedLifespan << 4 | encodedMaxIdle);
   }
}
