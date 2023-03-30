package org.infinispan.server.memcached.binary;

/**
 * @since 15.0
 **/
public enum BinaryCommand {
   // Opcodes
   GET(0x00),
   SET(0x01),
   ADD(0x02),
   REPLACE(0x03),
   DELETE(0x04),
   INCREMENT(0x05),
   DECREMENT(0x06),
   QUIT(0x07),
   FLUSH(0x08),
   GETQ(0x09),
   NO_OP(0x0a),
   VERSION(0x0b),
   GETK(0x0c),
   GETKQ(0x0d),
   APPEND(0x0e),
   PREPEND(0x0f),
   STAT(0x10),
   SETQ(0x11),
   ADDQ(0x12),
   REPLACEQ(0x13),
   DELETEQ(0x14),
   INCREMENTQ(0x15),
   DECREMENTQ(0x16),
   QUITQ(0x17),
   FLUSHQ(0x18),
   APPENDQ(0x19),
   PREPENDQ(0x1a),
   VERBOSITY(0x1b),
   TOUCH(0x1c),
   GAT(0x1d),
   GATQ(0x1e),
   SASL_LIST_MECHS(0x20),
   SASL_AUTH(0x21),
   SASL_STEP(0x22),
   GATK(0x23),
   GATKQ(0x24),
   CONFIG_GET(0x60);

   private final byte opCode;

   static final BinaryCommand[] ALL;

   static {
      ALL = new BinaryCommand[0x61];
      for (BinaryCommand op : values()) {
         ALL[op.opCode] = op;
      }
   }

   BinaryCommand(int opCode) {
      this.opCode = (byte) opCode;
   }

   public byte opCode() {
      return opCode;
   }

   public static BinaryCommand fromOpCode(byte opCode) {
      return ALL[opCode];
   }
}
