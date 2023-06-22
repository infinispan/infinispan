package org.infinispan.server.memcached.text;

import java.nio.charset.StandardCharsets;
import java.time.temporal.Temporal;

import org.infinispan.server.memcached.logging.Header;

/**
 * @since 15.0
 **/
public class TextHeader extends Header {
   private final TextCommand op;
   private final byte[] key;


   public TextHeader(int requestBytes, Temporal requestStart, String principalName, byte[] key, TextCommand op) {
      super(requestStart, requestBytes, principalName);
      this.key = key;
      this.op = op;
   }

   @Override
   public Object getKey() {
      if (key == null) return null;
      return new String(key, StandardCharsets.US_ASCII);
   }

   @Override
   public String getOp() {
      return op.name();
   }

   @Override
   public String getProtocol() {
      return "MCTXT"; // Memcached Text Protocol
   }
}
