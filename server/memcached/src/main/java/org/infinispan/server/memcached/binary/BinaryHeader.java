package org.infinispan.server.memcached.binary;

import java.time.temporal.Temporal;

import org.infinispan.server.memcached.logging.Header;

/**
 * @since 15.0
 **/
public class BinaryHeader extends Header {
   private final Object key;
   final BinaryCommand op;
   final int opaque;
   long cas;

   BinaryHeader(Temporal requestStart, int requestBytes, String principalName, Object key, BinaryCommand op, int opaque, long cas) {
      super(requestStart, requestBytes, principalName);
      this.key = key;
      this.op = op;
      this.opaque = opaque;
      this.cas = cas;
   }

   BinaryHeader(BinaryHeader h) {
      this(h.requestStart, h.requestBytes, h.principalName, h.key, h.op, h.opaque, h.cas);
   }

   @Override
   public Object getKey() {
      return key;
   }

   @Override
   public String getOp() {
      return op.name();
   }

   @Override
   public String getProtocol() {
      return "MCBIN";
   }
}
