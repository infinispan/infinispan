package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectOutput;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;

/**
 * Array backed, expandable {@link ObjectOutput} implementation.
 */
final class BytesObjectOutput implements ObjectOutput {

   final GlobalMarshaller marshaller;

   byte bytes[];
   int pos;

   BytesObjectOutput(int size, GlobalMarshaller marshaller) {
      this.bytes = new byte[size];
      this.marshaller = marshaller;
   }

   @Override
   public void writeObject(Object obj) throws IOException {
      marshaller.writeNullableObject(obj, this);
   }

   @Override
   public void write(int b) {
      writeByte(b);
   }

   @Override
   public void write(byte[] b) {
      final int len = b.length;
      final int newcount = ensureCapacity(len);
      System.arraycopy(b, 0, bytes, pos, len);
      pos = newcount;
   }

   @Override
   public void write(byte[] b, int off, int len) {
      final int newcount = ensureCapacity(len);
      System.arraycopy(b, off, bytes, pos, len);
      pos = newcount;
   }

   @Override
   public void writeBoolean(boolean v) {
      writeByte((byte) (v ? 1 : 0));
   }

   @Override
   public void writeByte(int v) {
      final int newcount = ensureCapacity(1);
      bytes[pos] = (byte) v;
      pos = newcount;
   }

   @Override
   public void writeShort(int v) {
      int newcount = ensureCapacity(2);
      final int s = pos;
      bytes[s] = (byte) (v >> 8);
      bytes[s+1] = (byte) v;
      pos = newcount;
   }

   @Override
   public void writeChar(int v) {
      int newcount = ensureCapacity(2);
      final int s = pos;
      bytes[s] = (byte) (v >> 8);
      bytes[s+1] = (byte) v;
      pos = newcount;
   }

   @Override
   public void writeInt(int v) {
      int newcount = ensureCapacity(4);
      final int s = pos;
      bytes[s] = (byte) (v >> 24);
      bytes[s+1] = (byte) (v >> 16);
      bytes[s+2] = (byte) (v >> 8);
      bytes[s+3] = (byte) v;
      pos = newcount;
   }

   @Override
   public void writeLong(long v) {
      int newcount = ensureCapacity(8);
      final int s = pos;
      bytes[s] = (byte) (v >> 56L);
      bytes[s+1] = (byte) (v >> 48L);
      bytes[s+2] = (byte) (v >> 40L);
      bytes[s+3] = (byte) (v >> 32L);
      bytes[s+4] = (byte) (v >> 24L);
      bytes[s+5] = (byte) (v >> 16L);
      bytes[s+6] = (byte) (v >> 8L);
      bytes[s+7] = (byte) v;
      pos = newcount;
   }

   @Override
   public void writeFloat(float v) {
      final int bits = Float.floatToIntBits(v);
      int newcount = ensureCapacity(4);
      final int s = pos;
      bytes[s] = (byte) (bits >> 24);
      bytes[s+1] = (byte) (bits >> 16);
      bytes[s+2] = (byte) (bits >> 8);
      bytes[s+3] = (byte) bits;
      pos = newcount;
   }

   @Override
   public void writeDouble(double v) {
      final long bits = Double.doubleToLongBits(v);
      int newcount = ensureCapacity(8);
      final int s = pos;
      bytes[s] = (byte) (bits >> 56L);
      bytes[s+1] = (byte) (bits >> 48L);
      bytes[s+2] = (byte) (bits >> 40L);
      bytes[s+3] = (byte) (bits >> 32L);
      bytes[s+4] = (byte) (bits >> 24L);
      bytes[s+5] = (byte) (bits >> 16L);
      bytes[s+6] = (byte) (bits >> 8L);
      bytes[s+7] = (byte) bits;
      pos = newcount;
   }

   @Override
   public void writeBytes(String s) {
      writeString(s);
   }

   @Override
   public void writeChars(String s) {
      writeString(s);
   }

   void writeString(String s) {
      int len;
      if ((len = s.length()) == 0){
         writeByte(0); // empty string
      } else if (isAscii(s, len)) {
         writeByte(1); // small ascii
         writeByte(len);
         int newcount = ensureCapacity(len);
         s.getBytes(0, len, bytes, pos);
         pos = newcount;
      } else {
         writeByte(2);  // large string
         writeUTF(s);
      }
   }

   private boolean isAscii(String s, int len) {
      boolean ascii = false;
      if(len < 64) {
         ascii = true;
         for (int i = 0; i < len; i++) {
            if (s.charAt(i) > 127) {
               ascii = false;
               break;
            }
         }
      }
      return ascii;
   }

   @Override
   public void writeUTF(String s) {
      int startPos = skipIntSize();
      int localPos = pos; /* avoid getfield opcode */
      byte[] localBuf = bytes; /* avoid getfield opcode */

      int strlen = s.length();
      int c = 0;

      int i=0;
      for (i=0; i<strlen; i++) {
         c = s.charAt(i);
         if (!((c >= 0x0001) && (c <= 0x007F))) break;

         if(localPos == bytes.length) {
            pos = localPos;
            ensureCapacity(1);
            localBuf = bytes;
         }
         localBuf[localPos++] = (byte) c;
      }

      for (;i < strlen; i++){
         c = s.charAt(i);
         if ((c >= 0x0001) && (c <= 0x007F)) {
            if(localPos == bytes.length) {
               pos = localPos;
               ensureCapacity(1);
               localBuf = bytes;
            }
            localBuf[localPos++] = (byte) c;

         } else if (c > 0x07FF) {
            if(localPos+3 >= bytes.length) {
               pos = localPos;
               ensureCapacity(3);
               localBuf = bytes;
            }

            localBuf[localPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
            localBuf[localPos++] = (byte) (0x80 | ((c >>  6) & 0x3F));
            localBuf[localPos++] = (byte) (0x80 | ((c >>  0) & 0x3F));
         } else {
            if(localPos + 2 >= bytes.length) {
               pos = localPos;
               ensureCapacity(2);
               localBuf = bytes;
            }

            localBuf[localPos++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
            localBuf[localPos++] = (byte) (0x80 | ((c >>  0) & 0x3F));
         }
      }
      pos = localPos;
      writeIntDirect(localPos - 4 - startPos, startPos);
   }

   private int skipIntSize() {
      ensureCapacity(4);
      int count = pos;
      pos +=4;
      return count;
   }

   private void writeIntDirect(int intValue, int index) {
      byte[] buf = bytes; /* avoid getfield opcode */
      buf[index] =   (byte) ((intValue >>> 24) & 0xFF);
      buf[index+1] = (byte) ((intValue >>> 16) & 0xFF);
      buf[index+2] = (byte) ((intValue >>>  8) & 0xFF);
      buf[index+3] = (byte) ((intValue >>>  0) & 0xFF);
   }

   @Override
   public void flush() {
      // No-op
   }

   @Override
   public void close() {
      // No-op
   }

   private int ensureCapacity(int len) {
      int newcount = pos + len;
      if (newcount > bytes.length) {
         byte newbuf[] = new byte[getNewBufferSize(bytes.length, newcount)];
         System.arraycopy(bytes, 0, newbuf, 0, pos);
         bytes = newbuf;
      }
      return newcount;
   }

   private static final int DEFAULT_DOUBLING_SIZE = 4 * 1024 * 1024; // 4MB

   /**
    * Gets the number of bytes to which the internal buffer should be resized.
    * If not enough space, it doubles the size until the internal buffer
    * reaches a configurable max size (default is 4MB), after which it begins
    * growing the buffer in 25% increments.  This is intended to help prevent
    * an OutOfMemoryError during a resize of a large buffer.
    *
    * @param curSize    the current number of bytes
    * @param minNewSize the minimum number of bytes required
    * @return the size to which the internal buffer should be resized
    */
   private int getNewBufferSize(int curSize, int minNewSize) {
      if (curSize <= DEFAULT_DOUBLING_SIZE)
         return Math.max(curSize << 1, minNewSize);
      else
         return Math.max(curSize + (curSize >> 2), minNewSize);
   }

   byte[] toBytes() {
      // Trim out unused bytes
      byte[] b = new byte[pos];
      System.arraycopy(bytes, 0, b, 0, pos);
      pos = 0;
      return b;
   }

   ByteBuffer toByteBuffer() {
      // No triming, just take position as length
      return new ByteBufferImpl(bytes, 0, pos);
   }

}
