package org.infinispan.server.resp.hll.internal;

import org.infinispan.commons.hash.MurmurHash3;

public class Util {

   private Util() { }

   public static long hash(byte[] data) {
      // The same seed from Redis' implementation.
      return MurmurHash3.MurmurHash3_x64_64(data, 0xadc83b19);
   }
}
