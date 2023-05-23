package org.infinispan.server.resp.commands;

import java.nio.charset.StandardCharsets;

/**
 * Utility class to transform byte[] arguments.
 *
 * @since 15.0
 */
public final class ArgumentUtils {

   private ArgumentUtils() {

   }
   public static long toLong(byte[] argument) {
      return Long.parseLong(new String(argument, StandardCharsets.US_ASCII));
   }

   public static int toInt(byte[] argument) {
      return Integer.parseInt(new String(argument, StandardCharsets.US_ASCII));
   }
}
