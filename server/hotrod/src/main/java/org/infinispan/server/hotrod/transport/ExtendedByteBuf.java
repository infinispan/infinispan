package org.infinispan.server.hotrod.transport;

import java.util.Optional;

import org.infinispan.commons.io.SignedNumeric;
import org.infinispan.commons.util.Util;
import org.infinispan.server.core.transport.VInt;
import org.infinispan.server.core.transport.VLong;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

public class ExtendedByteBuf {
   public static ByteBuf wrappedBuffer(byte[]... arrays) {
      return Unpooled.wrappedBuffer(arrays);
   }

   public static ByteBuf buffer(int capacity) {
      return Unpooled.buffer(capacity);
   }

   public static ByteBuf dynamicBuffer() {
      return Unpooled.buffer();
   }

   public static int readUnsignedShort(ByteBuf bf) {
      return bf.readUnsignedShort();
   }

   public static int readUnsignedInt(ByteBuf bf) {
      return VInt.read(bf);
   }

   public static long readUnsignedLong(ByteBuf bf) {
      return VLong.read(bf);
   }

   public static byte[] readRangedBytes(ByteBuf bf) {
      int length = readUnsignedInt(bf);
      return readRangedBytes(bf, length);
   }

   public static byte[] readRangedBytes(ByteBuf bf, int length) {
      if (length > 0) {
         byte[] array = new byte[length];
         bf.readBytes(array);
         return array;
      } else {
         return new byte[0];
      }
   }

   /**
    * Reads optional range of bytes. Negative lengths are translated to None, 0 length represents empty Array
    */
   public static Optional<byte[]> readOptRangedBytes(ByteBuf bf) {
      int length = SignedNumeric.decode(readUnsignedInt(bf));
      return length < 0 ? Optional.empty() : Optional.of(readRangedBytes(bf, length));
   }

   /**
    * Reads an optional String. 0 length is an empty string, negative length is translated to None.
    */
   public static Optional<String> readOptString(ByteBuf bf) {
      Optional<byte[]> bytes = readOptRangedBytes(bf);
      return bytes.map(b -> new String(b, CharsetUtil.UTF_8));
   }

   /**
    * Reads length of String and then returns an UTF-8 formatted String of such length. If the length is 0, an empty
    * String is returned.
    */
   public static String readString(ByteBuf bf) {
      byte[] bytes = readRangedBytes(bf);
      return bytes.length > 0 ? new String(bytes, CharsetUtil.UTF_8) : "";
   }

   /**
    * Reads a byte if possible.  If not present the reader index is reset to the last mark.
    *
    * @param bf
    * @return
    */
   public static Optional<Byte> readMaybeByte(ByteBuf bf) {
      if (bf.readableBytes() >= 1) {
         return Optional.of(bf.readByte());
      } else {
         bf.resetReaderIndex();
         return Optional.empty();
      }
   }

   public static Optional<Long> readMaybeLong(ByteBuf bf) {
      if (bf.readableBytes() < 8) {
         bf.resetReaderIndex();
         return Optional.empty();
      } else {
         return Optional.of(bf.readLong());
      }
   }

   /**
    * Reads a variable long if possible.  If not present the reader index is reset to the last mark.
    *
    * @param bf
    * @return
    */
   public static Optional<Long> readMaybeVLong(ByteBuf bf) {
      if (bf.readableBytes() >= 1) {
         byte b = bf.readByte();

         return read(bf, b, 7, (long) b & 0x7F, 1);
      } else {
         bf.resetReaderIndex();
         return Optional.empty();
      }
   }

   private static Optional<Long> read(ByteBuf buf, byte b, int shift, long i, int count) {
      if ((b & 0x80) == 0) return Optional.of(i);
      else {
         if (count > 9)
            throw new IllegalStateException(
                  "Stream corrupted.  A variable length long cannot be longer than 9 bytes.");

         if (buf.readableBytes() >= 1) {
            byte bb = buf.readByte();
            return read(buf, bb, shift + 7, i | (bb & 0x7FL) << shift, count + 1);
         } else {
            buf.resetReaderIndex();
            return Optional.empty();
         }
      }
   }

   /**
    * Reads a variable size int if possible.  If not present the reader index is reset to the last mark.
    *
    * @param bf
    * @return
    */
   public static Optional<Integer> readMaybeVInt(ByteBuf bf) {
      if (bf.readableBytes() >= 1) {
         byte b = bf.readByte();
         return read(bf, b, 7, b & 0x7F, 1);
      } else {
         bf.resetReaderIndex();
         return Optional.empty();
      }
   }

   private static Optional<Integer> read(ByteBuf buf, byte b, int shift, int i, int count) {
      if ((b & 0x80) == 0) return Optional.of(i);
      else {
         if (count > 5)
            throw new IllegalStateException(
                  "Stream corrupted.  A variable length integer cannot be longer than 5 bytes.");

         if (buf.readableBytes() >= 1) {
            byte bb = buf.readByte();
            return read(buf, bb, shift + 7, i | (int) ((bb & 0x7FL) << shift), count + 1);
         } else {
            buf.resetReaderIndex();
            return Optional.empty();
         }
      }
   }

   /**
    * Reads a range of bytes if possible.  If not present the reader index is reset to the last mark.
    *
    * @param bf
    * @return
    */
   public static Optional<byte[]> readMaybeRangedBytes(ByteBuf bf) {
      Optional<Integer> length = readMaybeVInt(bf);
      if (length.isPresent()) {
         int l = length.get();
         if (bf.readableBytes() >= l) {
            if (l > 0) {
               byte[] array = new byte[l];
               bf.readBytes(array);
               return Optional.of(array);
            } else {
               return Optional.of(Util.EMPTY_BYTE_ARRAY);
            }
         } else {
            bf.resetReaderIndex();
            return Optional.empty();
         }
      } else return Optional.empty();
   }

   public static Optional<byte[]> readMaybeRangedBytes(ByteBuf bf, int length) {
      if (bf.readableBytes() < length) {
         bf.resetReaderIndex();
         return Optional.empty();
      } else {
         byte[] bytes = new byte[length];
         bf.readBytes(bytes);
         return Optional.of(bytes);
      }
   }

   public static Optional<Integer> readMaybeSignedInt(ByteBuf bf) {
      return readMaybeVInt(bf).map(SignedNumeric::decode);
   }

   /**
    * Read a range of bytes prefixed by its length (encoded as a signed VInt).
    *
    * @return {@code Optional(Optional(byte[])} if it could read the range,
    * {@code Optional(Optional.empty())} if the length was negative,
    * or {@code Optional.empty()} if the input buffer didn't contain the entire range.
    */
   public static Optional<Optional<byte[]>> readMaybeOptRangedBytes(ByteBuf bf) {
      Optional<Integer> l = readMaybeSignedInt(bf);
      if (l.isPresent()) {
         int length = l.get();
         if (length < 0) {
            return Optional.of(Optional.empty());
         } else {
            Optional<byte[]> rb = readMaybeRangedBytes(bf, length);
            if (rb.isPresent()) {
               return Optional.of(rb);
            } else {
               return Optional.empty();
            }
         }
      } else {
         return Optional.empty();
      }
   }

   /**
    * Reads a string if possible.  If not present the reader index is reset to the last mark.
    *
    * @param bf
    * @return
    */
   public static Optional<String> readMaybeString(ByteBuf bf) {
      Optional<byte[]> bytes = readMaybeRangedBytes(bf);
      return bytes.map(b -> {
         if (b.length == 0) return "";
         else return new String(b, CharsetUtil.UTF_8);
      });
   }

   public static Optional<Optional<String>> readMaybeOptString(ByteBuf bf) {
      return readMaybeOptRangedBytes(bf).map(optionalBytes -> optionalBytes.map(
            bytes -> {
               if (bytes.length == 0) return "";
               else return new String(bytes, CharsetUtil.UTF_8);
            }));
   }

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

   public static void writeString(String msg, ByteBuf bf) {
      writeRangedBytes(msg.getBytes(CharsetUtil.UTF_8), bf);
   }

   public static void writeString(Optional<String> msg, ByteBuf bf) {
      writeRangedBytes(msg.map(m -> m.getBytes(CharsetUtil.UTF_8)).orElse(Util.EMPTY_BYTE_ARRAY), bf);
   }
}
