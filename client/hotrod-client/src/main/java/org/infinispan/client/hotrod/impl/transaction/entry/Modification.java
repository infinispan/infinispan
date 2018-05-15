package org.infinispan.client.hotrod.impl.transaction.entry;

import static org.infinispan.client.hotrod.impl.transport.netty.ByteBufUtil.writeArray;

import java.util.concurrent.TimeUnit;

import org.infinispan.client.hotrod.impl.protocol.Codec;

import io.netty.buffer.ByteBuf;

/**
 * The final modification of a specific key.
 *
 * @author Pedro Ruivo
 * @since 9.3
 */
public class Modification {

   private final byte[] key;
   private final byte[] value;
   private final long versionRead;
   private final long lifespan;
   private final long maxIdle;
   private final TimeUnit lifespanTimeUnit;
   private final TimeUnit maxIdleTimeUnit;
   private final byte control;

   Modification(byte[] key, byte[] value, long versionRead, long lifespan, long maxIdle, TimeUnit lifespanTimeUnit,
         TimeUnit maxIdleTimeUnit, byte control) {
      this.key = key;
      this.value = value;
      this.versionRead = versionRead;
      this.lifespan = lifespan;
      this.maxIdle = maxIdle;
      this.lifespanTimeUnit = lifespanTimeUnit;
      this.maxIdleTimeUnit = maxIdleTimeUnit;
      this.control = control;
   }


   /**
    * Writes this modification to the {@link ByteBuf}.
    *
    * @param byteBuf the {@link ByteBuf} to write to.
    * @param codec   the {@link Codec} to use.
    */
   public void writeTo(ByteBuf byteBuf, Codec codec) {
      writeArray(byteBuf, key);
      byteBuf.writeByte(control);
      if (!ControlByte.NON_EXISTING.hasFlag(control) && !ControlByte.NOT_READ.hasFlag(control)) {
         byteBuf.writeLong(versionRead);
      }
      if (ControlByte.REMOVE_OP.hasFlag(control)) {
         return;
      }
      codec.writeExpirationParams(byteBuf, lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      writeArray(byteBuf, value);

   }

   /**
    * The estimated size.
    *
    * @param codec the {@link Codec} to use for the size estimation.
    * @return the estimated size.
    */
   public int estimateSize(Codec codec) {
      int size = key.length + 1; //key + control
      if (!ControlByte.NON_EXISTING.hasFlag(control) && !ControlByte.NOT_READ.hasFlag(control)) {
         size += 8; //long
      }
      if (!ControlByte.REMOVE_OP.hasFlag(control)) {
         size += value.length;
         size += codec.estimateExpirationSize(lifespan, lifespanTimeUnit, maxIdle, maxIdleTimeUnit);
      }
      return size;
   }
}
