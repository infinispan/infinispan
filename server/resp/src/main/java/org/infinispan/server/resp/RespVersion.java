package org.infinispan.server.resp;

/**
 * The RESP version. Determines the format of the responses
 *
 * @since 15.2
 */
public enum RespVersion {
   RESP2,
   RESP3;

   public static RespVersion of(int version) {
      if (version == 3) {
         return RESP3;
      }
      throw new IllegalArgumentException();
   }
}
