package org.infinispan.client.hotrod.impl.transport.netty;

import static org.infinispan.commons.io.SignedNumeric.encode;

import javax.transaction.xa.Xid;

import org.infinispan.client.hotrod.impl.protocol.HotRodConstants;
import org.infinispan.client.hotrod.transaction.manager.RemoteXid;
import org.infinispan.commons.netty.VarintEncodeDecode;
import org.infinispan.commons.util.Util;

import io.netty.buffer.ByteBuf;

/**
 * Helper methods for writing varints, arrays and strings to {@link ByteBuf}.
 */
public final class ByteBufUtil {
   private ByteBufUtil() {}

   public static byte[] readArray(ByteBuf buf) {
      int length = readVInt(buf);
      byte[] bytes = new byte[length];
      buf.readBytes(bytes, 0, length);
      return bytes;
   }

   public static String readString(ByteBuf buf) {
      byte[] strContent = readArray(buf);
      return new String(strContent, HotRodConstants.HOTROD_STRING_CHARSET);
   }

   public static void writeString(ByteBuf buf, String string) {
      if (string != null && !string.isEmpty()) {
         writeArray(buf, string.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      } else {
         writeVInt(buf, 0);
      }
   }

   public static void writeOptionalString(ByteBuf buf, String string) {
      if (string == null) {
         writeSignedVInt(buf, -1);
      } else {
         writeOptionalArray(buf, string.getBytes(HotRodConstants.HOTROD_STRING_CHARSET));
      }
   }

   public static void writeArray(ByteBuf buf, byte[] toAppend) {
      writeVInt(buf, toAppend.length);
      buf.writeBytes(toAppend);
   }

   public static void writeArray(ByteBuf buf, byte[] toAppend, int offset, int count) {
      writeVInt(buf, count);
      buf.writeBytes(toAppend, offset, count);
   }

   public static int estimateArraySize(byte[] array) {
      return estimateVIntSize(array.length) + array.length;
   }

   public static int estimateVIntSize(int value) {
      return (32 - Integer.numberOfLeadingZeros(value)) / 7 + 1;
   }

   public static void writeOptionalArray(ByteBuf buf, byte[] toAppend) {
      writeSignedVInt(buf, toAppend.length);
      buf.writeBytes(toAppend);
   }

   public static void writeVInt(ByteBuf buf, int i) {
      VarintEncodeDecode.writeVInt(buf, i);
   }

   public static void writeSignedVInt(ByteBuf buf, int i) {
      writeVInt(buf, encode(i));
   }

   public static void writeVLong(ByteBuf buf, long i) {
      VarintEncodeDecode.writeVLong(buf, i);
   }

   public static int estimateVLongSize(long value) {
      return (64 - Long.numberOfLeadingZeros(value)) / 7 + 1;
   }

   public static long readVLong(ByteBuf buf) {
      int before = buf.readerIndex();
      long value = VarintEncodeDecode.readVLong(buf);
      if (before == buf.readerIndex())
         throw HintedReplayingDecoder.REPLAY;
      return value;
   }

   public static int readVInt(ByteBuf buf) {
      int before = buf.readerIndex();
      int value = VarintEncodeDecode.readVInt(buf);
      if (before == buf.readerIndex())
         throw HintedReplayingDecoder.REPLAY;
      return value;
   }

   public static String limitedHexDump(ByteBuf buf) {
      return Util.hexDump(buf::getByte, buf.readerIndex(), buf.readableBytes());
   }

   /**
    * Estimates the {@link Xid} encoding size.
    * <p>
    * If the instance is a {@link RemoteXid}, the estimation is accurate. Otherwise, the max size is used.
    *
    * @param xid the {@link Xid} instance to test.
    * @return the estimated size.
    */
   public static int estimateXidSize(Xid xid) {
      if (xid instanceof RemoteXid) {
         return ((RemoteXid) xid).estimateSize();
      } else {
         // Worst case.
         // To be more accurate, we need to invoke getGlobalTransactionId and getBranchQualifier that will most likely
         //create and copy the array
         return estimateVIntSize(xid.getFormatId()) + Xid.MAXBQUALSIZE + Xid.MAXGTRIDSIZE;
      }
   }

   /**
    * Writes the {@link Xid} to the {@link ByteBuf}.
    *
    * @param buf the buffer to write to.
    * @param xid the {@link Xid} to encode
    */
   public static void writeXid(ByteBuf buf, Xid xid) {
      if (xid instanceof RemoteXid) {
         ((RemoteXid) xid).writeTo(buf);
      } else {
         ByteBufUtil.writeSignedVInt(buf, xid.getFormatId());
         writeArray(buf, xid.getGlobalTransactionId());
         writeArray(buf, xid.getBranchQualifier());
      }
   }
}
