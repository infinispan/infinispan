package org.infinispan.commons.marshall;

import java.io.IOException;
import java.io.ObjectOutput;
import java.io.OutputStream;

/**
 * Class that extends {@link OutputStream} and implements {@link ObjectOutput}.
 * <p>
 * All the methods delegates to a {@link ObjectOutput} implementation.
 *
 * @author Pedro Ruivo
 * @since 8.2
 */
public class DelegatingObjectOutput extends OutputStream implements ObjectOutput {

   protected final ObjectOutput objectOutput;

   public DelegatingObjectOutput(ObjectOutput objectOutput) {
      this.objectOutput = objectOutput;
   }

   @Override
   public void writeObject(Object obj) throws IOException {
      objectOutput.writeObject(obj);
   }

   @Override
   public void write(int b) throws IOException {
      objectOutput.write(b);
   }

   @Override
   public void write(byte[] b) throws IOException {
      objectOutput.write(b);
   }

   @Override
   public void write(byte[] b, int off, int len) throws IOException {
      objectOutput.write(b, off, len);
   }

   @Override
   public void flush() throws IOException {
      objectOutput.flush();
   }

   @Override
   public void close() throws IOException {
      objectOutput.close();
   }

   @Override
   public void writeBoolean(boolean v) throws IOException {
      objectOutput.writeBoolean(v);
   }

   @Override
   public void writeByte(int v) throws IOException {
      objectOutput.writeByte(v);
   }

   @Override
   public void writeShort(int v) throws IOException {
      objectOutput.writeShort(v);
   }

   @Override
   public void writeChar(int v) throws IOException {
      objectOutput.writeChar(v);
   }

   @Override
   public void writeInt(int v) throws IOException {
      objectOutput.writeInt(v);
   }

   @Override
   public void writeLong(long v) throws IOException {
      objectOutput.writeLong(v);
   }

   @Override
   public void writeFloat(float v) throws IOException {
      objectOutput.writeFloat(v);
   }

   @Override
   public void writeDouble(double v) throws IOException {
      objectOutput.writeDouble(v);
   }

   @Override
   public void writeBytes(String s) throws IOException {
      objectOutput.writeBytes(s);
   }

   @Override
   public void writeChars(String s) throws IOException {
      objectOutput.writeChars(s);
   }

   @Override
   public void writeUTF(String s) throws IOException {
      objectOutput.writeUTF(s);
   }
}
