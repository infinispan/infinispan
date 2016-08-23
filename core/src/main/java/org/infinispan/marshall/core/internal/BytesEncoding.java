package org.infinispan.marshall.core.internal;

/**
 * Basic encoding/decoding of primitives
 */
final class BytesEncoding implements Encoding<BytesObjectOutput, BytesObjectInput> {

   public BytesEncoding() {
   }

   @Override
   public void encodeByte(int b, BytesObjectOutput out) {
      int newcount = checkCapacity(1, out);
      out.bytes[out.pos] = (byte) b;
      out.pos = newcount;
   }

   @Override
   public void encodeBoolean(boolean b, BytesObjectOutput out) {
      encodeByte((byte) (b ? 1 : 0), out);
   }

   @Override
   public void encodeBytes(byte[] b, int off, int len, BytesObjectOutput out) {
      int newcount = checkCapacity(len, out);
      System.arraycopy(b, off, out.bytes, out.pos, len);
      out.pos = newcount;
   }

   @Override
   public void encodeChar(int v, BytesObjectOutput out) {
      int newcount = checkCapacity(2, out);
      final int s = out.pos;
      out.bytes[s] = (byte) (v >> 8);
      out.bytes[s+1] = (byte) v;
      out.pos = newcount;
   }

   @Override
   public void encodeDouble(double v, BytesObjectOutput out) {
      final long bits = Double.doubleToLongBits(v);
      int newcount = checkCapacity(8, out);
      final int s = out.pos;
      out.bytes[s] = (byte) (bits >> 56L);
      out.bytes[s+1] = (byte) (bits >> 48L);
      out.bytes[s+2] = (byte) (bits >> 40L);
      out.bytes[s+3] = (byte) (bits >> 32L);
      out.bytes[s+4] = (byte) (bits >> 24L);
      out.bytes[s+5] = (byte) (bits >> 16L);
      out.bytes[s+6] = (byte) (bits >> 8L);
      out.bytes[s+7] = (byte) bits;
      out.pos = newcount;
   }

   @Override
   public void encodeFloat(float v, BytesObjectOutput out) {
      final int bits = Float.floatToIntBits(v);
      int newcount = checkCapacity(4, out);
      final int s = out.pos;
      out.bytes[s] = (byte) (bits >> 24);
      out.bytes[s+1] = (byte) (bits >> 16);
      out.bytes[s+2] = (byte) (bits >> 8);
      out.bytes[s+3] = (byte) bits;
      out.pos = newcount;
   }

   @Override
   public void encodeInt(int v, BytesObjectOutput out) {
      int newcount = checkCapacity(4, out);
      final int s = out.pos;
      out.bytes[s] = (byte) (v >> 24);
      out.bytes[s+1] = (byte) (v >> 16);
      out.bytes[s+2] = (byte) (v >> 8);
      out.bytes[s+3] = (byte) v;
      out.pos = newcount;
   }

   @Override
   public void encodeLong(long v, BytesObjectOutput out) {
      int newcount = checkCapacity(8, out);
      final int s = out.pos;
      out.bytes[s] = (byte) (v >> 56L);
      out.bytes[s+1] = (byte) (v >> 48L);
      out.bytes[s+2] = (byte) (v >> 40L);
      out.bytes[s+3] = (byte) (v >> 32L);
      out.bytes[s+4] = (byte) (v >> 24L);
      out.bytes[s+5] = (byte) (v >> 16L);
      out.bytes[s+6] = (byte) (v >> 8L);
      out.bytes[s+7] = (byte) v;
      out.pos = newcount;
   }

   @Override
   public void encodeShort(int v, BytesObjectOutput out) {
      int newcount = checkCapacity(2, out);
      final int s = out.pos;
      out.bytes[s] = (byte) (v >> 8);
      out.bytes[s+1] = (byte) v;
      out.pos = newcount;
   }

   @Override
   public void encodeString(String s, BytesObjectOutput out) {
      int len;
      if ((len = s.length()) == 0){
         encodeByte(0, out); // empty string
      } else if (isAscii(s, len)) {
         encodeByte(1, out); // small ascii
         encodeByte(len, out);
         int newcount = checkCapacity(len, out);
         s.getBytes(0, len, out.bytes, out.pos);
         out.pos = newcount;
      } else {
         encodeByte(2, out);  // large string
         encodeStringUtf8(s, out);
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
   public void encodeStringUtf8(String s, BytesObjectOutput out) {
      int startPos = skipIntSize(out);
      int localPos = out.pos; /* avoid getfield opcode */
      byte[] localBuf = out.bytes; /* avoid getfield opcode */

      int strlen = s.length();
      int c = 0;

      int i=0;
      for (i=0; i<strlen; i++) {
         c = s.charAt(i);
         if (!((c >= 0x0001) && (c <= 0x007F))) break;

         if(localPos == out.bytes.length) {
            out.pos = localPos;
            checkCapacity(1, out);
            localBuf = out.bytes;
         }
         localBuf[localPos++] = (byte) c;
      }

      for (;i < strlen; i++){
         c = s.charAt(i);
         if ((c >= 0x0001) && (c <= 0x007F)) {
            if(localPos == out.bytes.length) {
               out.pos = localPos;
               checkCapacity(1, out);
               localBuf = out.bytes;
            }
            localBuf[localPos++] = (byte) c;

         } else if (c > 0x07FF) {
            if(localPos+3 >= out.bytes.length) {
               out.pos = localPos;
               checkCapacity(3, out);
               localBuf = out.bytes;
            }

            localBuf[localPos++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
            localBuf[localPos++] = (byte) (0x80 | ((c >>  6) & 0x3F));
            localBuf[localPos++] = (byte) (0x80 | ((c >>  0) & 0x3F));
         } else {
            if(localPos + 2 >= out.bytes.length) {
               out.pos = localPos;
               checkCapacity(2, out);
               localBuf = out.bytes;
            }

            localBuf[localPos++] = (byte) (0xC0 | ((c >>  6) & 0x1F));
            localBuf[localPos++] = (byte) (0x80 | ((c >>  0) & 0x3F));
         }
      }
      out.pos = localPos;
      writeIntDirect(localPos - 4 - startPos, startPos, out);
   }

   @Override
   public int encodeEmpty(int num, BytesObjectOutput out) {
      int posBefore = out.pos;
      out.pos = checkCapacity(num, out);
      return posBefore;
   }

   @Override
   public int encodePosition(BytesObjectOutput out, int offset) {
      out.bytes[offset] = (byte) (out.pos >> 24);
      out.bytes[offset+1] = (byte) (out.pos >> 16);
      out.bytes[offset+2] = (byte) (out.pos >> 8);
      out.bytes[offset+3] = (byte) out.pos;
      return out.pos;
   }

   private int skipIntSize(BytesObjectOutput out) {
      checkCapacity(4, out);
      int count = out.pos;
      out.pos +=4;
      return count;
   }

   private void writeIntDirect(int intValue, int index, BytesObjectOutput out) {
      byte[] buf = out.bytes; /* avoid getfield opcode */
      buf[index] =   (byte) ((intValue >>> 24) & 0xFF);
      buf[index+1] = (byte) ((intValue >>> 16) & 0xFF);
      buf[index+2] = (byte) ((intValue >>>  8) & 0xFF);
      buf[index+3] = (byte) ((intValue >>>  0) & 0xFF);
   }

   private int checkCapacity(int len, BytesObjectOutput out) {
      int newcount = out.pos + len;
      if (newcount > out.bytes.length) {
         byte newbuf[] = new byte[getNewBufferSize(out.bytes.length, newcount)];
         System.arraycopy(out.bytes, 0, newbuf, 0, out.pos);
         out.bytes = newbuf;
      }
      return newcount;
   }

   private static final int DEFAULT_DOUBLING_SIZE = 4 * 1024 * 1024; // 4MB

   /**
    * Gets the number of bytes to which the internal buffer should be resized.
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

   @Override
   public boolean decodeBoolean(BytesObjectInput in) {
      return decodeByte(in) != 0;
   }

   @Override
   public byte decodeByte(BytesObjectInput in) {
      return in.bytes[in.pos++];
   }

   @Override
   public void decodeBytes(byte[] b, int off, int len, BytesObjectInput in) {
      System.arraycopy(in.bytes, in.pos, b, off, len);
      in.pos += len;
   }

   @Override
   public char decodeChar(BytesObjectInput in) {
      char v = (char) (in.bytes[in.pos] << 8 | (in.bytes[in.pos + 1] & 0xff));
      in.pos += 2;
      return v;
   }

   @Override
   public double decodeDouble(BytesObjectInput in) {
      return Double.longBitsToDouble(decodeLong(in));
   }

   @Override
   public float decodeFloat(BytesObjectInput in) {
      return Float.intBitsToFloat(decodeInt(in));
   }

   @Override
   public int decodeInt(BytesObjectInput in) {
      int v = in.bytes[in.pos] << 24
            | (in.bytes[in.pos + 1] & 0xff) << 16
            | (in.bytes[in.pos + 2] & 0xff) << 8
            | (in.bytes[in.pos + 3] & 0xff);
      in.pos = in.pos + 4;
      return v;
   }

   @Override
   public long decodeLong(BytesObjectInput in) {
      return (long) decodeInt(in) << 32L | (long) decodeInt(in) & 0xffffffffL;
   }

   @Override
   public short decodeShort(BytesObjectInput in) {
      short v = (short) (in.bytes[in.pos] << 8 | (in.bytes[in.pos + 1] & 0xff));
      in.pos += 2;
      return v;
   }

   @Override
   public String decodeString(BytesObjectInput in) {
      byte mark = decodeByte(in);

      switch(mark) {
         case 0:
            return ""; // empty string
         case 1:
            // small ascii
            int size = decodeByte(in);
            String str = new String(in.bytes, 0, in.pos, size);
            in.pos += size;
            return str;
         case 2:
            // large string
            return decodeStringUtf8(in);
         case 3:
         default:
            throw new RuntimeException("Unkwown marker(String). mark=" + mark);
      }
   }

   @Override
   public String decodeStringUtf8(BytesObjectInput in) {
      int utflen = decodeInt(in);

      byte[] bytearr = in.bytes;
      char[] chararr = new char[utflen];

      utflen = in.pos + utflen;
      int c, char2, char3;
      int count = in.pos;
      int chararr_count=0;

      while (count < utflen) {
         c = (int) bytearr[count] & 0xff;
         if (c > 127) break;
         count++;
         chararr[chararr_count++]=(char)c;
      }

      while (count < utflen) {
         c = (int) bytearr[count] & 0xff;
         switch (c >> 4) {
            case 0: case 1: case 2: case 3: case 4: case 5: case 6: case 7:
                    /* 0xxxxxxx*/
               count++;
               chararr[chararr_count++]=(char)c;
               break;
            case 12: case 13:
                    /* 110x xxxx   10xx xxxx*/
               count += 2;
               if (count > utflen)
                  throw new RuntimeException(
                        "malformed input: partial character at end");
               char2 = (int) bytearr[count-1];
               if ((char2 & 0xC0) != 0x80)
                  throw new RuntimeException(
                        "malformed input around byte " + count);
               chararr[chararr_count++]=(char)(((c & 0x1F) << 6) |
                                                     (char2 & 0x3F));
               break;
            case 14:
                    /* 1110 xxxx  10xx xxxx  10xx xxxx */
               count += 3;
               if (count > utflen)
                  throw new RuntimeException(
                        "malformed input: partial character at end");
               char2 = (int) bytearr[count-2];
               char3 = (int) bytearr[count-1];
               if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                  throw new RuntimeException(
                        "malformed input around byte " + (count-1));
               chararr[chararr_count++]=(char)(((c     & 0x0F) << 12) |
                                                     ((char2 & 0x3F) << 6)  |
                                                     ((char3 & 0x3F) << 0));
               break;
            default:
                    /* 10xx xxxx,  1111 xxxx */
               throw new RuntimeException(
                     "malformed input around byte " + count);
         }
      }

      in.pos = count;

      return new String(chararr, 0, chararr_count);
   }

   @Override
   public int decodeUnsignedShort(BytesObjectInput in) {
      int v = (in.bytes[in.pos] & 0xff) << 8 | (in.bytes[in.pos + 1] & 0xff);
      in.pos += 2;
      return v;
   }

   @Override
   public void decodeRewind(BytesObjectInput in, int pos) {
      if (in.offset == 0)
         in.pos = pos;
      else
         in.pos = pos + in.offset;
   }

}
