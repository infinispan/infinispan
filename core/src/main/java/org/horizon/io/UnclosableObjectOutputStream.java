package org.horizon.io;

import java.io.IOException;
import java.io.ObjectOutput;

/**
 * An unclosable version of an {@link java.io.ObjectOutput}.  This delegates all methods except {@link #flush()} and
 * {@link #close()}.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public class UnclosableObjectOutputStream implements ObjectOutput {

   private final ObjectOutput delegate;

   public UnclosableObjectOutputStream(ObjectOutput delegate) {
      this.delegate = delegate;
   }

   public final void writeObject(Object obj) throws IOException {
      delegate.writeObject(obj);
   }

   public final void write(int b) throws IOException {
      delegate.write(b);
   }

   public final void write(byte[] b) throws IOException {
      delegate.write(b);
   }

   public final void write(byte[] b, int off, int len) throws IOException {
      delegate.write(b, off, len);
   }

   public final void writeBoolean(boolean v) throws IOException {
      delegate.writeBoolean(v);
   }

   public final void writeByte(int v) throws IOException {
      delegate.writeByte(v);
   }

   public final void writeShort(int v) throws IOException {
      delegate.writeShort(v);
   }

   public final void writeChar(int v) throws IOException {
      delegate.writeChar(v);
   }

   public final void writeInt(int v) throws IOException {
      delegate.writeInt(v);
   }

   public final void writeLong(long v) throws IOException {
      delegate.writeLong(v);
   }

   public final void writeFloat(float v) throws IOException {
      delegate.writeFloat(v);
   }

   public final void writeDouble(double v) throws IOException {
      delegate.writeDouble(v);
   }

   public final void writeBytes(String s) throws IOException {
      delegate.writeBytes(s);
   }

   public final void writeChars(String s) throws IOException {
      delegate.writeChars(s);
   }

   public final void writeUTF(String str) throws IOException {
      delegate.writeUTF(str);
   }

   public final void flush() throws IOException {
      throw new UnsupportedOperationException("flush() not supported in an UnclosableObjectOutputStream!");
   }

   public final void close() throws IOException {
      throw new UnsupportedOperationException("close() not supported in an UnclosableObjectOutputStream!");
   }
}
