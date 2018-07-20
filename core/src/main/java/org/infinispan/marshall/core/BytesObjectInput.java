package org.infinispan.marshall.core;

import java.io.EOFException;
import java.io.IOException;

import org.infinispan.commons.marshall.UserObjectInput;

/**
 * Array backed {@link UserObjectInput} implementation.
 *
 * {@link #skip(long)} and {@link #skipBytes(int)} have been enhanced so that
 * if a negative number is passed in, they skip backwards effectively
 * providing rewind capabilities.
 */
final class BytesObjectInput extends AbstractUserObjectInput {

   final byte bytes[];

   final GlobalMarshaller marshaller;

   int pos;
   int offset; // needed for external JBoss Marshalling, to be able to rewind correctly when the bytes are prepended :(

   private BytesObjectInput(byte[] bytes, int offset, GlobalMarshaller marshaller) {
      this.bytes = bytes;
      this.pos = offset;
      this.offset = offset;
      this.marshaller = marshaller;
   }

   static BytesObjectInput from(byte[] bytes, GlobalMarshaller marshaller) {
      return from(bytes, 0, marshaller);
   }

   static BytesObjectInput from(byte[] bytes, int offset, GlobalMarshaller marshaller) {
      return new BytesObjectInput(bytes, offset, marshaller);
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      return marshaller.readNullableObject(this);
   }

   @Override
   public int read() {
      if (pos >= bytes.length) {
         return -1;
      }
      return bytes[pos++] & 0xff;
   }

   @Override
   public int read(byte[] b) {
      return read(b, 0, b.length);
   }

   @Override
   public int read(byte[] b, int off, int len) {
      if (pos >= bytes.length) {
         return -1;
      }
      if (pos + len >= bytes.length) {
         len = bytes.length - pos;
      }
      System.arraycopy(bytes, pos, b, off, len);
      pos += len;
      return len;
   }

   @Override
   public long skip(long n) {
      if (n > 0) {
         long skip = bytes.length - pos;
         if (skip > n)
            skip = n;

         pos += skip;
         return skip;
      } else {
         int idx = Math.min(bytes.length, pos);
         long skip = idx + n;
         // Calculate max to avoid skipping before offset
         pos = (int) Math.max(skip, offset);
         return skip;
      }
   }

   @Override
   public int available() {
      return bytes.length - pos;
   }

   @Override
   public void close() {
      // No-op
   }

   @Override
   public void readFully(byte[] b) {
      readFully(b, 0, b.length);
   }

   @Override
   public void readFully(byte[] b, int off, int len) {
      System.arraycopy(bytes, pos, b, off, len);
      pos += len;
   }

   @Override
   public int skipBytes(int n) {
      return (int) skip(n);
   }

   @Override
   public boolean readBoolean() throws EOFException {
      return readByte() != 0;
   }

   @Override
   public byte readByte() throws EOFException {
      if (pos >= bytes.length) {
         throw new EOFException();
      }
      return bytes[pos++];
   }

   @Override
   public int readUnsignedByte() throws EOFException {
      return readByte() & 0xff;
   }

   @Override
   public short readShort() {
      short v = (short) (bytes[pos] << 8 | (bytes[pos + 1] & 0xff));
      pos += 2;
      return v;
   }

   @Override
   public int readUnsignedShort() {
      int v = (bytes[pos] & 0xff) << 8 | (bytes[pos + 1] & 0xff);
      pos += 2;
      return v;
   }

   @Override
   public char readChar() {
      char v = (char) (bytes[pos] << 8 | (bytes[pos + 1] & 0xff));
      pos += 2;
      return v;
   }

   @Override
   public int readInt() {
      int v = bytes[pos] << 24
            | (bytes[pos + 1] & 0xff) << 16
            | (bytes[pos + 2] & 0xff) << 8
            | (bytes[pos + 3] & 0xff);
      pos = pos + 4;
      return v;
   }

   @Override
   public long readLong() {
      return (long) readInt() << 32L | (long) readInt() & 0xffffffffL;
   }

   @Override
   public float readFloat() {
      return Float.intBitsToFloat(readInt());
   }

   @Override
   public double readDouble() {
      return Double.longBitsToDouble(readLong());
   }

   @Override
   public String readLine() {
      return null;
   }

   @Override
   public String readUTF() {
      int utflen = readInt();

      byte[] bytearr = bytes;
      char[] chararr = new char[utflen];

      utflen = pos + utflen;
      int c, char2, char3;
      int count = pos;
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
                     (char3 & 0x3F));
               break;
            default:
                    /* 10xx xxxx,  1111 xxxx */
               throw new RuntimeException(
                     "malformed input around byte " + count);
         }
      }

      pos = count;

      return new String(chararr, 0, chararr_count);
   }

   String readString() throws EOFException {
      int mark = readByte();

      switch(mark) {
         case 0:
            return ""; // empty string
         case 1:
            // small ascii
            int size = readByte();
            String str = new String(bytes, 0, pos, size);
            pos += size;
            return str;
         case 2:
            // large string
            return readUTF();
         default:
            throw new RuntimeException("Unknown marker(String). mark=" + mark);
      }
   }

   @Override
   public Object readUserObject() throws ClassNotFoundException, IOException {
      return marshaller.readUserObject(this);
   }
}
