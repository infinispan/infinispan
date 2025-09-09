package org.infinispan.server.memcached.text;

import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;
import java.util.Collection;

import org.infinispan.commons.util.Util;
import org.infinispan.server.memcached.logging.Header;

/**
 * @since 15.0
 **/
public class TextHeader extends Header {
   private final String op;
   private final byte[] key;
   private final Collection<byte[]> keys;


   public TextHeader(int requestBytes, Temporal requestStart, String principalName, byte[] key, Collection<byte[]> keys, String op) {
      super(requestStart, requestBytes, principalName);
      this.key = key;
      this.keys = keys;
      this.op = op;
   }

   @Override
   public Object getKey() {
      if (key != null)
         return new String(key, StandardCharsets.US_ASCII);
      if (keys != null)
         return keys.toArray(Util.EMPTY_BYTE_ARRAY_ARRAY);
      return null;
   }

   @Override
   public String getOp() {
      return op;
   }

   @Override
   public String getProtocol() {
      return "MCTXT"; // Memcached Text Protocol
   }
}
