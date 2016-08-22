package org.infinispan.marshall.core.internal;

import org.infinispan.commons.marshall.Externalizer;

import java.io.IOException;
import java.io.ObjectInput;

final class BytesObjectInput implements ObjectInput, PositionalBuffer.Input {

   final byte bytes[];
   final InternalMarshaller internal;

   int pos;

   private BytesObjectInput(byte[] bytes, int offset, InternalMarshaller internal) {
      this.bytes = bytes;
      this.pos = offset;
      this.internal = internal;
   }

   static BytesObjectInput from(byte[] bytes, InternalMarshaller internal) {
      return from(bytes, 0, internal);
   }

   static BytesObjectInput from(byte[] bytes, int offset, InternalMarshaller internal) {
      return new BytesObjectInput(bytes, offset, internal);
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      Externalizer<Object> ext = internal.externalizers.findReadExternalizer(this);
      if (ext != null)
         return ext.readObject(this);
      else {
         try {
            return internal.external.objectFromObjectStream(this);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }
      }
   }

   @Override
   public int read() {
      return readUnsignedByte();
   }

   @Override
   public int read(byte[] b) {
      readFully(b);
      return b.length;
   }

   @Override
   public int read(byte[] b, int off, int len) {
      readFully(b, off, len);
      return len;
   }

   @Override
   public long skip(long n) {
      long skip = bytes.length - pos;
      if (skip < pos)
         skip = n < 0 ? 0 : n;

      pos += skip;
      return skip;
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
      internal.enc.decodeBytes(b, 0, b.length, this);
   }

   @Override
   public void readFully(byte[] b, int off, int len) {
      internal.enc.decodeBytes(b, off, len, this);
   }

   @Override
   public int skipBytes(int n) {
      return (int) skip(n);
   }

   @Override
   public boolean readBoolean() {
      return internal.enc.decodeBoolean(this);
   }

   @Override
   public byte readByte() {
      return internal.enc.decodeByte(this);
   }

   @Override
   public int readUnsignedByte() {
      return readByte() & 0xff;
   }

   @Override
   public short readShort() {
      return internal.enc.decodeShort(this);
   }

   @Override
   public int readUnsignedShort() {
      return internal.enc.decodeUnsignedShort(this);
   }

   @Override
   public char readChar() {
      return internal.enc.decodeChar(this);
   }

   @Override
   public int readInt() {
      return internal.enc.decodeInt(this);
   }

   @Override
   public long readLong() {
      return internal.enc.decodeLong(this);
   }

   @Override
   public float readFloat() {
      return internal.enc.decodeFloat(this);
   }

   @Override
   public double readDouble() {
      return internal.enc.decodeDouble(this);
   }

   @Override
   public String readLine() {
      return null;
   }

   @Override
   public String readUTF() {
      return internal.enc.decodeStringUtf8(this);
   }

   @Override
   public void rewindPosition(int pos) {
      internal.enc.decodeRewind(this, pos);
   }

}
