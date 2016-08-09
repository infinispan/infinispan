package org.infinispan.marshall.core.internal;

import org.infinispan.commons.marshall.AdvancedExternalizer;

import java.io.IOException;
import java.io.ObjectOutput;

final class ByteArrayObjectOutput implements ObjectOutput {

   final InternalMarshaller internal;

   byte bytes[];
   int pos;

   public ByteArrayObjectOutput(int size, InternalMarshaller internal) {
      this.bytes = new byte[size];
      this.internal = internal;
   }

   @Override
   public void writeObject(Object obj) throws IOException {
      AdvancedExternalizer<Object> ext = internal.externalizers.findWriteExternalizer(obj, this);
      if (ext != null) {
         ext.writeObject(this, obj);
      } else {
         // TODO: Forward to external marshaller...
      }
   }

   @Override
   public void write(int b) {
      internal.enc.encodeByte(b, this);
   }

   @Override
   public void write(byte[] b) {
      internal.enc.encodeBytes(b, 0, b.length, this);
   }

   @Override
   public void write(byte[] b, int off, int len) {
      internal.enc.encodeBytes(b, off, len, this);
   }

   @Override
   public void writeBoolean(boolean v) {
      internal.enc.encodeBoolean(v, this);
   }

   @Override
   public void writeByte(int v) {
      internal.enc.encodeByte(v, this);
   }

   @Override
   public void writeShort(int v) {
      internal.enc.encodeShort(v, this);
   }

   @Override
   public void writeChar(int v) {
      internal.enc.encodeChar(v, this);
   }

   @Override
   public void writeInt(int v) {
      internal.enc.encodeInt(v, this);
   }

   @Override
   public void writeLong(long v) {
      internal.enc.encodeLong(v, this);
   }

   @Override
   public void writeFloat(float v) {
      internal.enc.encodeFloat(v, this);
   }

   @Override
   public void writeDouble(double v) {
      internal.enc.encodeDouble(v, this);
   }

   @Override
   public void writeBytes(String s) {
      internal.enc.encodeString(s, this);
   }

   @Override
   public void writeChars(String s) {
      internal.enc.encodeString(s, this);
   }

   @Override
   public void writeUTF(String s) {
      internal.enc.encodeStringUtf8(s, this);
   }

   @Override
   public void flush() {
      // No-op
   }

   @Override
   public void close() {
      // No-op
   }

//   ByteBuffer toByteBuffer() {
//      return new ByteBufferImpl(bytes, 0, bytes.length);
//   }

   byte[] toBytes() {
      byte[] b = new byte[pos];
      System.arraycopy(bytes, 0, b, 0, pos);
      pos = 0;
      return b;
   }

}
