package org.infinispan.server.memcached.binary;

import java.time.temporal.Temporal;

import org.infinispan.server.memcached.logging.Header;

/**
 * @since 15.0
 **/
public class BinaryHeader extends Header {

   private Object key;
   private BinaryCommand op;
   private int opaque;
   private long cas;

   private BinaryHeader(Temporal requestStart, int requestBytes, String principalName, Object key, BinaryCommand op, int opaque, long cas) {
      super(requestStart, requestBytes, principalName);
      this.key = key;
      this.op = op;
      this.opaque = opaque;
      this.cas = cas;
   }

   BinaryHeader() {
      this(null, -1, null, null, null, -1, -1);
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


   public BinaryHeader replace(Temporal requestStart, int requestBytes, String principalName, Object key, BinaryCommand op, int opaque, long cas) {
      this.requestStart = requestStart;
      this.requestBytes = requestBytes;
      this.principalName = principalName;
      this.key = key;
      this.op = op;
      this.opaque = opaque;
      this.cas = cas;
      return this;
   }

   public BinaryCommand getCommand() {
      return op;
   }

   public int getOpaque() {
      return opaque;
   }

   public long getCas() {
      return cas;
   }

   public void setCas(long cas) {
      this.cas = cas;
   }
}
