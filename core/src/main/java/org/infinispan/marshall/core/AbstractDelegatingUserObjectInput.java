package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * Abstract class which extends {@link AbstractUserObjectInput} and implements all {@link ObjectInput} methods via the
 * provided delegate implementation.
 *
 * @author remerson
 * @since 9.4
 */
abstract class AbstractDelegatingUserObjectInput extends AbstractUserObjectInput {

   private final ObjectInput delegate;

   AbstractDelegatingUserObjectInput(ObjectInput delegate) {
      this.delegate = delegate;
   }

   public Object readObject() throws ClassNotFoundException, IOException {
      return delegate.readObject();
   }

   public int read() throws IOException {
      return delegate.read();
   }

   public int read(byte[] bytes) throws IOException {
      return delegate.read(bytes);
   }

   public int read(byte[] bytes, int i, int i1) throws IOException {
      return delegate.read(bytes, i, i1);
   }

   public long skip(long l) throws IOException {
      return delegate.skip(l);
   }

   public int available() throws IOException {
      return delegate.available();
   }

   public void close() throws IOException {
      delegate.close();
   }

   public void readFully(byte[] bytes) throws IOException {
      delegate.readFully(bytes);
   }

   public void readFully(byte[] bytes, int i, int i1) throws IOException {
      delegate.readFully(bytes, i, i1);
   }

   public int skipBytes(int i) throws IOException {
      return delegate.skipBytes(i);
   }

   public boolean readBoolean() throws IOException {
      return delegate.readBoolean();
   }

   public byte readByte() throws IOException {
      return delegate.readByte();
   }

   public int readUnsignedByte() throws IOException {
      return delegate.readUnsignedByte();
   }

   public short readShort() throws IOException {
      return delegate.readShort();
   }

   public int readUnsignedShort() throws IOException {
      return delegate.readUnsignedShort();
   }

   public char readChar() throws IOException {
      return delegate.readChar();
   }

   public int readInt() throws IOException {
      return delegate.readInt();
   }

   public long readLong() throws IOException {
      return delegate.readLong();
   }

   public float readFloat() throws IOException {
      return delegate.readFloat();
   }

   public double readDouble() throws IOException {
      return delegate.readDouble();
   }

   public String readLine() throws IOException {
      return delegate.readLine();
   }

   public String readUTF() throws IOException {
      return delegate.readUTF();
   }
}
