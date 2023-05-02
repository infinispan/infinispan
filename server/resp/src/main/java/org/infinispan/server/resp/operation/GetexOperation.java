package org.infinispan.server.resp.operation;

import java.nio.charset.StandardCharsets;
import java.util.List;

import org.infinispan.commons.time.TimeService;
import org.infinispan.server.resp.Util;

public class GetexOperation {
   private static final byte[] PERSIST_BYTES = "PERSIST".getBytes(StandardCharsets.US_ASCII);

   public static long parseExpiration(List<byte[]> arguments, TimeService timeService) {
      long expirationMs = 0;
      // Below here we parse the optional expiration for the GETEX command:
      // Related parameters:
      //
      // `EX` seconds: TTL in seconds;
      // `PX` milliseconds: TTL in ms;
      // `EXAT` timestamp: Unix time for key expiration, seconds;
      // `PXAT` timestamp: Unix time for key expiration, milliseconds;
      // `PERSIST`: Remove the TTL for the entry.
      //
      // Each of the time arguments are exclusive, only one is present at a time.
      // All these arguments can be in any order. Expiration must be followed by the
      // proper value.
      for (int i = 1; i < arguments.size(); i++) {
         byte[] arg = arguments.get(i);
         // expiration options
         if (arg.length == 2 || arg.length == 4) {
            switch (arg[0]) {
               case 'E':
               case 'P':
               case 'e':
               case 'p':
                  // Throws an exception if invalid.
                  RespExpiration expiration = RespExpiration.valueOf(arg);
                  if (expirationMs != 0)
                     throw new IllegalArgumentException("Only one expiration option should be used on GETEX");
                  if (i + 1 > arguments.size())
                     throw new IllegalArgumentException("No argument accompanying expiration");
                  expirationMs = expiration
                        .convert(Long.parseLong(new String(arguments.get(i + 1), StandardCharsets.US_ASCII)), timeService);
                  i++;
                  continue;
            }
         }
         // `PERSIST` argument.
         if (arg.length == 7 && Util.isAsciiBytesEquals(PERSIST_BYTES, arg)) {
            if (expirationMs != 0)
               throw new IllegalArgumentException("PERSIST and EX/PX/EXAT/PXAT are mutually exclusive");
            expirationMs = -1;
            continue;
         }
         throw new IllegalArgumentException("Unknown argument for GETEX operation");
      }
      return expirationMs;
   }
}
