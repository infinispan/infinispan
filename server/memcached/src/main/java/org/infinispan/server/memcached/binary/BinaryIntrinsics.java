package org.infinispan.server.memcached.binary;

import org.infinispan.server.core.transport.ExtendedByteBufJava;

import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.TooLongFrameException;

public class BinaryIntrinsics {

   public static int int_(ByteBuf b) {
      if (b.readableBytes() >= 4) {
         return b.readInt();
      } else return 0;
   }

   public static long long_(ByteBuf b) {
      if (b.readableBytes() >= 8) {
         return b.readLong();
      } else return 0;
   }

   public static byte byte_(ByteBuf b) {
      if (b.isReadable()) {
         return b.readByte();
      } else return 0;
   }

   public static short short_(ByteBuf b) {
      if (b.readableBytes() >= 2) {
         return b.readShort();
      } else return 0;
   }

   public static byte[] fixedArray(ByteBuf b, int length, int maxArrayLength) {
      if (maxArrayLength >= 0 && length > maxArrayLength) {
         throw new TooLongFrameException("Array length " + length + " exceeded " + maxArrayLength);
      }
      b.markReaderIndex();
      return ExtendedByteBufJava.readMaybeRangedBytes(b, length);
   }

   public static BinaryCommand opCode(ByteBuf b) {
      if (b.isReadable()) {
         return BinaryCommand.fromOpCode(b.readByte());
      } else {
         return null;
      }
   }
}
