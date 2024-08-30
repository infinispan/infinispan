package org.infinispan.commons.netty;

import io.netty.buffer.ByteBuf;

public final class VarintEncodeDecode {

   private VarintEncodeDecode() { }

   public static void writeVInt(ByteBuf buf, int i) {
      while ((i & ~0x7F) != 0) {
         buf.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      buf.writeByte((byte) i);
   }

   public static int readVInt(ByteBuf buf) {
      return BranchlessParser.readRawVarint32(buf);
   }

   public static void writeVLong(ByteBuf buf, long i) {
      while ((i & ~0x7F) != 0) {
         buf.writeByte((byte) ((i & 0x7f) | 0x80));
         i >>>= 7;
      }
      buf.writeByte((byte) i);
   }

   public static long readVLong(ByteBuf buf) {
      return BranchlessParser.readRawVarint64(buf);
   }
}
