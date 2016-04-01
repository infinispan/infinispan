package org.infinispan.server.core;

import io.netty.buffer.ByteBuf;

import java.util.OptionalInt;
import java.util.OptionalLong;

/**
 * Static helper class to provide unsigned numeric methods when using a {@link ByteBuf}.
 *
 * @author wburns
 * @since 9.0
 */
public class UnsignedNumeric {
   /**
    * Reads an int stored in variable-length format.  Reads between one and five bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   public static int readUnsignedInt(ByteBuf in) {
      byte b = in.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Same as {@link #readUnsignedInt(ByteBuf)} except that it returns an <code>OptionalInt</code> that can be used
    * when the byte buf is not used in blocking mode.  If an empty value is returned the reader index will be reset.
    * @param in the byte buf to read from
    * @return an optional showing whether or not the int could be read
    */
   public static OptionalInt readOptionalUnsignedInt(ByteBuf in) {
      if (in.readableBytes() < 1) {
         in.resetReaderIndex();
         return OptionalInt.empty();
      }
      byte b = in.readByte();
      int i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         if (in.readableBytes() < 1) {
            in.resetReaderIndex();
            return OptionalInt.empty();
         }
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return OptionalInt.of(i);
   }

   /**
    * Reads a long stored in variable-length format.  Reads between one and nine bytes.  Smaller values take fewer
    * bytes.  Negative numbers are not supported.
    */
   public static long readUnsignedLong(ByteBuf in) {
      byte b = in.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return i;
   }

   /**
    * Same as {@link #readUnsignedLong(ByteBuf)} (ByteBuf)} except that it returns an <code>OptionalLong</code> that
    * can be used when the byte buf is not used in blocking mode.  If an empty value is returned the reader index will
    * be reset.
    * @param in the byte buf to read from
    * @return an optional showing whether or not the long could be read
    */
   public static OptionalLong readOptionalUnsignedLong(ByteBuf in) {
      if (in.readableBytes() < 1) {
         in.resetReaderIndex();
         return OptionalLong.empty();
      }
      byte b = in.readByte();
      long i = b & 0x7F;
      for (int shift = 7; (b & 0x80) != 0; shift += 7) {
         if (in.readableBytes() < 1) {
            in.resetReaderIndex();
            return OptionalLong.empty();
         }
         b = in.readByte();
         i |= (b & 0x7FL) << shift;
      }
      return OptionalLong.of(i);
   }
}
