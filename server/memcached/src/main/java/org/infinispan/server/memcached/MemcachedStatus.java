package org.infinispan.server.memcached;

import java.nio.charset.StandardCharsets;

/**
 * @since 15.0
 **/
public enum MemcachedStatus {
   NO_ERROR(0x0000),
   KEY_NOT_FOUND(0x0001, "NOT_FOUND"),
   KEY_EXISTS(0x0002, "EXISTS"),
   VALUE_TOO_LARGE(0x0003),
   INVALID_ARGUMENTS(0x0004),
   ITEM_NOT_STORED(0x0005, "NOT_STORED"),
   INCR_DECR_NON_NUMERIC(0x0006),
   VBUCKET_ON_OTHER_SERVER(0x0007),
   AUTHN_ERROR(0x0020),
   AUTHN_CONTINUE(0x0021),
   UNKNOWN_COMMAND(0x0081),
   OUT_OF_MEMORY(0x0082),
   NOT_SUPPORTED(0x0083),
   INTERNAL_ERROR(0x0084, "SERVER_ERROR"),
   BUSY(0x0085),
   TEMPORARY_FAILURE(0x0086),
   DELETED(0x0);

   private final short binaryOpCode;
   private final byte[] text;

   MemcachedStatus(int binaryOpCode, String text) {
      this.binaryOpCode = (short) binaryOpCode;
      this.text = text.getBytes(StandardCharsets.US_ASCII);
   }

   MemcachedStatus(int binaryOpCode) {
      this.binaryOpCode = (short) binaryOpCode;
      this.text = null;
   }

   public short getBinary() {
      return binaryOpCode;
   }

   public byte[] getText() {
      return text;
   }
}
