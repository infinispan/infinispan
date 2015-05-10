package org.infinispan.client.hotrod.impl.protocol;

import java.util.concurrent.TimeUnit;

/**
 * @author gustavonalle
 * @since 8.0
 */
public final class CodecUtils {

   private CodecUtils() {
   }

   public static boolean isIntCompatible(long value) {
      int narrowed = (int) value;
      return narrowed == value;
   }

   public static int toSeconds(long duration, TimeUnit timeUnit) {
      int seconds = (int) timeUnit.toSeconds(duration);
      long inverseDuration = timeUnit.convert(seconds, TimeUnit.SECONDS);

      if (duration > inverseDuration) {
         //Round up.
         seconds++;
      }
      return seconds;
   }

}
