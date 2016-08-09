package org.infinispan.marshall.core.internal;

import org.infinispan.commons.marshall.AdvancedExternalizer;

import java.io.IOException;
import java.io.ObjectInput;

final class ByteArrayObjectInput implements ObjectInput {

   final byte bytes[];
   final InternalMarshaller internal;

   int pos;

   ByteArrayObjectInput(byte[] bytes, InternalMarshaller internal) {
      this.bytes = bytes;
      this.internal = internal;
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      AdvancedExternalizer<Object> ext = internal.externalizers.findReadExternalizer(this);
      if (ext != null)
         return ext.readObject(this);

      // TODO: How to deal with null values?

      // TODO: Forward to external marshaller?
      return null;
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
      return -1;
   }

   @Override
   public int available() {
      return -1;
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
      return -1;
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

}
