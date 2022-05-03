package org.infinispan.hotrod.impl.transaction.entry;

import static org.infinispan.hotrod.impl.transport.netty.ByteBufUtil.writeArray;

import org.infinispan.api.common.CacheEntryExpiration;
import org.infinispan.hotrod.impl.protocol.Codec;

import io.netty.buffer.ByteBuf;

/**
 * The final modification of a specific key.
 *
 * @since 14.0
 */
public class Modification {

   private final byte[] key;
   private final byte[] value;
   private final long versionRead;
   private final CacheEntryExpiration.Impl expiration;
   private final byte control;

   Modification(byte[] key, byte[] value, long versionRead, CacheEntryExpiration expiration, byte control) {
      this.key = key;
      this.value = value;
      this.versionRead = versionRead;
      this.expiration = (CacheEntryExpiration.Impl) expiration;
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
      codec.writeExpirationParams(byteBuf, expiration);
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
         size += codec.estimateExpirationSize(expiration);
      }
      return size;
   }

   /**
    * @return The key changed by this modification.
    */
   public byte[] getKey() {
      return key;
   }
}
