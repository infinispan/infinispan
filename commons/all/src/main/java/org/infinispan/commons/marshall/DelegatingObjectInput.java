package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;

/**
 * Class that extends {@link InputStream} and implements {@link ObjectInput}.
 * <p>
 * All the methods delegates to a {@link ObjectInput} implementation.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class DelegatingObjectInput extends InputStream implements ObjectInput {

   protected final ObjectInput objectInput;

   public DelegatingObjectInput(ObjectInput objectInput) {
      this.objectInput = objectInput;
   }

   @Override
   public Object readObject() throws ClassNotFoundException, IOException {
      return objectInput.readObject();
   }

   @Override
   public int read() throws IOException {
      return objectInput.read();
   }

   @Override
   public int read(byte[] b) throws IOException {
      return objectInput.read(b);
   }

   @Override
   public int read(byte[] b, int off, int len) throws IOException {
      return objectInput.read(b, off, len);
   }

   @Override
   public long skip(long n) throws IOException {
      return objectInput.skip(n);
   }

   @Override
   public int available() throws IOException {
      return objectInput.available();
   }

   @Override
   public void close() throws IOException {
      objectInput.close();
   }

   @Override
   public void readFully(byte[] b) throws IOException {
      objectInput.readFully(b);
   }

   @Override
   public void readFully(byte[] b, int off, int len) throws IOException {
      objectInput.readFully(b, off, len);
   }

   @Override
   public int skipBytes(int n) throws IOException {
      return objectInput.skipBytes(n);
   }

   @Override
   public boolean readBoolean() throws IOException {
      return objectInput.readBoolean();
   }

   @Override
   public byte readByte() throws IOException {
      return objectInput.readByte();
   }

   @Override
   public int readUnsignedByte() throws IOException {
      return objectInput.readUnsignedByte();
   }

   @Override
   public short readShort() throws IOException {
      return objectInput.readShort();
   }

   @Override
   public int readUnsignedShort() throws IOException {
      return objectInput.readUnsignedShort();
   }

   @Override
   public char readChar() throws IOException {
      return objectInput.readChar();
   }

   @Override
   public int readInt() throws IOException {
      return objectInput.readInt();
   }

   @Override
   public long readLong() throws IOException {
      return objectInput.readLong();
   }

   @Override
   public float readFloat() throws IOException {
      return objectInput.readFloat();
   }

   @Override
   public double readDouble() throws IOException {
      return objectInput.readDouble();
   }

   @Override
   public String readLine() throws IOException {
      return objectInput.readLine();
   }

   @Override
   public String readUTF() throws IOException {
      return objectInput.readUTF();
   }
}
