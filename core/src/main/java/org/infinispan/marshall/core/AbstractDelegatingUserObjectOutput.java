package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * Abstract class which extends {@link AbstractUserObjectOutput} and implements all {@link ObjectOutput} methods via the
 * provided delegate implementation.
 *
 * @author remerson
 * @since 9.4
 */
abstract class AbstractDelegatingUserObjectOutput extends AbstractUserObjectOutput {

   private final ObjectOutput delegate;

   AbstractDelegatingUserObjectOutput(ObjectOutput delegate) {
      this.delegate = delegate;
   }

   @Override
   public void writeObject(Object o) throws IOException {
      delegate.writeObject(o);
   }

   @Override
   public void write(int i) throws IOException {
      delegate.write(i);
   }

   @Override
   public void write(byte[] bytes) throws IOException {
      delegate.write(bytes);
   }

   @Override
   public void write(byte[] bytes, int i, int i1) throws IOException {
      delegate.write(bytes, i, i1);
   }

   @Override
   public void flush() throws IOException {
      delegate.flush();
   }

   @Override
   public void close() throws IOException {
      delegate.close();
   }

   @Override
   public void writeBoolean(boolean b) throws IOException {
      delegate.writeBoolean(b);
   }

   @Override
   public void writeByte(int i) throws IOException {
      delegate.writeByte(i);
   }

   @Override
   public void writeShort(int i) throws IOException {
      delegate.writeShort(i);
   }

   @Override
   public void writeChar(int i) throws IOException {
      delegate.writeChar(i);
   }

   @Override
   public void writeInt(int i) throws IOException {
      delegate.writeInt(i);
   }

   @Override
   public void writeLong(long l) throws IOException {
      delegate.writeLong(l);
   }

   @Override
   public void writeFloat(float v) throws IOException {
      delegate.writeFloat(v);
   }

   @Override
   public void writeDouble(double v) throws IOException {
      delegate.writeDouble(v);
   }

   @Override
   public void writeBytes(String s) throws IOException {
      delegate.writeBytes(s);
   }

   @Override
   public void writeChars(String s) throws IOException {
      delegate.writeChars(s);
   }

   @Override
   public void writeUTF(String s) throws IOException {
      delegate.writeUTF(s);
   }
}
