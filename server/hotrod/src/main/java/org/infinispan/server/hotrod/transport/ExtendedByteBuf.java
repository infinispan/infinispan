package org.infinispan.server.hotrod.transport;

import java.nio.ByteBuffer;

import org.infinispan.commons.io.SignedNumeric;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.core.transport.VInt;
import org.infinispan.server.core.transport.VLong;

import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;

public class ExtendedByteBuf {

   public static void writeUnsignedShort(int i, ByteBuf bf) {
      bf.writeShort(i);
   }

   public static void writeUnsignedInt(int i, ByteBuf bf) {
      VInt.write(bf, i);
   }

   public static void writeUnsignedLong(long l, ByteBuf bf) {
      VLong.write(bf, l);
   }

   public static void writeRangedBytes(byte[] src, ByteBuf bf) {
      writeUnsignedInt(src.length, bf);
      if (src.length > 0)
         bf.writeBytes(src);
   }

   public static void writeRangedBytes(byte[] src, int offset, ByteBuf bf) {
      int l = src.length - offset;
      writeUnsignedInt(l, bf);
      if (l > 0)
         bf.writeBytes(src);
   }

   public static void writeRangedBytes(ByteBuffer src, ByteBuf bf) {
      writeUnsignedInt(src.remaining(), bf);
      bf.writeBytes(src);
   }

   public static void writeString(String msg, ByteBuf bf) {
      writeRangedBytes(msg.getBytes(CharsetUtil.UTF_8), bf);
   }

   public static void writeXid(XidImpl xid, ByteBuf buffer) {
      VInt.write(buffer, SignedNumeric.encode(xid.getFormatId()));
      //avoid allocating/copying arrays
      writeRangedBytes(xid.getGlobalTransactionIdAsByteBuffer(), buffer);
      writeRangedBytes(xid.getBranchQualifierAsByteBuffer(), buffer);
   }
}
