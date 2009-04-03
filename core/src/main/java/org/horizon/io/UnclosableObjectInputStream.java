package org.horizon.io;

import java.io.IOException;
import java.io.ObjectInput;

/**
 * A delegating {@link java.io.ObjectInput} that delegates all methods except {@link ObjectInput#close()}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnclosableObjectInputStream implements ObjectInput {
   private final ObjectInput delegate;

   public UnclosableObjectInputStream(ObjectInput delegate) {
      this.delegate = delegate;
   }

   public final Object readObject() throws ClassNotFoundException, IOException {
      return delegate.readObject();
   }

   public final int read() throws IOException {
      return delegate.read();
   }

   public final int read(byte[] b) throws IOException {
      return delegate.read(b);
   }

   public final int read(byte[] b, int off, int len) throws IOException {
      return delegate.read(b, off, len);
   }

   public final long skip(long n) throws IOException {
      return delegate.skip(n);
   }

   public final int available() throws IOException {
      return delegate.available();
   }

   public final void close() throws IOException {
      throw new UnsupportedOperationException("close() is not supported in an UnclosableObjectInputStream!");
   }

   public final void readFully(byte[] b) throws IOException {
      delegate.readFully(b);
   }

   public final void readFully(byte[] b, int off, int len) throws IOException {
      delegate.readFully(b, off, len);
   }

   public final int skipBytes(int n) throws IOException {
      return delegate.skipBytes(n);
   }

   public final boolean readBoolean() throws IOException {
      return delegate.readBoolean();
   }

   public final byte readByte() throws IOException {
      return delegate.readByte();
   }

   public final int readUnsignedByte() throws IOException {
      return delegate.readUnsignedByte();
   }

   public final short readShort() throws IOException {
      return delegate.readShort();
   }

   public final int readUnsignedShort() throws IOException {
      return delegate.readUnsignedShort();
   }

   public final char readChar() throws IOException {
      return delegate.readChar();
   }

   public final int readInt() throws IOException {
      return delegate.readInt();
   }

   public final long readLong() throws IOException {
      return delegate.readLong();
   }

   public final float readFloat() throws IOException {
      return delegate.readFloat();
   }

   public final double readDouble() throws IOException {
      return delegate.readDouble();
   }

   public final String readLine() throws IOException {
      return delegate.readLine();
   }

   public final String readUTF() throws IOException {
      return delegate.readUTF();
   }
}
