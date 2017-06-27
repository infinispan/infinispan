package org.infinispan.server.hotrod.test;

import static org.infinispan.server.hotrod.transport.ExtendedByteBuf.writeUnsignedLong;

import org.infinispan.server.hotrod.transport.ExtendedByteBuf;
import org.infinispan.server.hotrod.tx.ControlByte;

import io.netty.buffer.ByteBuf;

/**
 * A transaction write for testing.
 *
 * @author Pedro Ruivo
 * @since 9.1
 */
public class TxWrite {
   private final long versionRead;
   private final byte control;
   private final byte[] value;
   private final int lifespan;
   private final int maxIdle;
   private final byte[] key;

   private TxWrite(long versionRead, byte control, byte[] value, int lifespan, int maxIdle, byte[] key) {
      this.versionRead = versionRead;
      this.control = control;
      this.value = value;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.key = key;
   }

   public static TxWrite put(byte[] key, byte[] value, int lifespan, int maxIdle, byte control, long versionRead) {
      return new TxWrite(versionRead, control, value, lifespan, maxIdle, key);
   }

   public static TxWrite remove(byte[] key, byte control, long versionRead) {
      return new TxWrite(versionRead, ControlByte.REMOVE_OP.set(control), null, 0, 0, key);
   }

   void encodeTo(ByteBuf buffer) {
      ExtendedByteBuf.writeRangedBytes(key, buffer);
      buffer.writeByte(control);
      if (!ControlByte.NOT_READ.hasFlag(control) && !ControlByte.NON_EXISTING.hasFlag(control)) {
         buffer.writeLong(versionRead);
      }
      if (ControlByte.REMOVE_OP.hasFlag(control)) {
         return;
      }
      if (lifespan > 0 || maxIdle > 0) {
         buffer.writeByte(0); // seconds for both
         writeUnsignedLong(lifespan, buffer); // lifespan
         writeUnsignedLong(maxIdle, buffer); // maxIdle
      } else {
         buffer.writeByte(0x88);
      }
      ExtendedByteBuf.writeRangedBytes(value, buffer);
   }
}
