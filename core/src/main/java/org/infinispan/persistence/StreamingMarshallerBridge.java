package org.infinispan.persistence;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.MarshallingException;
import org.infinispan.marshall.persistence.PersistenceMarshaller;

/**
 * A bridge between the {@link PersistenceMarshaller} and the deprecated {@link StreamingMarshaller} interface.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
class StreamingMarshallerBridge implements StreamingMarshaller {

   final PersistenceMarshaller marshaller;

   StreamingMarshallerBridge(PersistenceMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      return new Output(os);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      try {
         oo.flush();
      } catch (IOException e) {
         // ignored
      }
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      out.writeObject(obj);
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return new Input(is);
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      try {
         oi.close();
      } catch (IOException e) {
         // ignored
      }
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return in.readObject();
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      return marshaller.readObject(is);
   }

   @Override
   public void stop() {
      marshaller.stop();
   }

   @Override
   public void start() {
      marshaller.start();
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(obj, estimatedSize);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(obj);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(buf);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(buf, offset, length);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return marshaller.objectToBuffer(o);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return marshaller.isMarshallable(o);
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return marshaller.getBufferSizePredictor(o);
   }

   @Override
   public MediaType mediaType() {
      return marshaller.mediaType();
   }

   private class Output implements ObjectOutput {

      final OutputStream out;

      Output(OutputStream out) {
         this.out = out;
      }

      @Override
      public void writeObject(Object o) throws IOException {
         marshaller.writeObject(o, out);
      }

      @Override
      public void write(int i) throws IOException {
         marshaller.writeObject(i, out);
      }

      @Override
      public void write(byte[] bytes) throws IOException {
         marshaller.writeObject(bytes, out);
      }

      @Override
      public void write(byte[] bytes, int i, int i1) throws IOException {
         marshaller.writeObject(bytes, out);
      }

      @Override
      public void flush() throws IOException {
         out.flush();
      }

      @Override
      public void close() throws IOException {
         out.close();
      }

      @Override
      public void writeBoolean(boolean b) throws IOException {
         marshaller.writeObject(b, out);
      }

      @Override
      public void writeByte(int i) throws IOException {
         marshaller.writeObject(i, out);
      }

      @Override
      public void writeShort(int i) throws IOException {
         marshaller.writeObject(i, out);
      }

      @Override
      public void writeChar(int i) throws IOException {
         marshaller.writeObject(i, out);
      }

      @Override
      public void writeInt(int i) throws IOException {
         marshaller.writeObject(i, out);
      }

      @Override
      public void writeLong(long l) throws IOException {
         marshaller.writeObject(l, out);
      }

      @Override
      public void writeFloat(float v) throws IOException {
         marshaller.writeObject(v, out);
      }

      @Override
      public void writeDouble(double v) throws IOException {
         marshaller.writeObject(v, out);
      }

      @Override
      public void writeBytes(String s) throws IOException {
         marshaller.writeObject(s, out);
      }

      @Override
      public void writeChars(String s) throws IOException {
         marshaller.writeObject(s, out);
      }

      @Override
      public void writeUTF(String s) throws IOException {
         marshaller.writeObject(s, out);
      }
   }

   private class Input implements ObjectInput {

      final InputStream in;

      public Input(InputStream in) {
         this.in = in;
      }

      @Override
      public Object readObject() throws ClassNotFoundException, IOException {
         return marshaller.readObject(in);
      }

      @Override
      public int read() throws IOException {
         return get();
      }

      @Override
      public int read(byte[] bytes) throws IOException {
         return get();
      }

      @Override
      public int read(byte[] bytes, int i, int i1) throws IOException {
         return get();
      }

      @Override
      public long skip(long l) throws IOException {
         return get();
      }

      @Override
      public int available() throws IOException {
         return get();
      }

      @Override
      public void close() throws IOException {
         in.close();
      }

      @Override
      public void readFully(byte[] bytes) throws IOException {
         in.read(bytes);
      }

      @Override
      public void readFully(byte[] bytes, int i, int i1) throws IOException {
         in.read(bytes, i, i1);
      }

      @Override
      public int skipBytes(int i) throws IOException {
         return (int) in.skip(i);
      }

      @Override
      public boolean readBoolean() throws IOException {
         return get();
      }

      @Override
      public byte readByte() throws IOException {
         return get();
      }

      @Override
      public int readUnsignedByte() throws IOException {
         return get();
      }

      @Override
      public short readShort() throws IOException {
         return get();
      }

      @Override
      public int readUnsignedShort() throws IOException {
         return get();
      }

      @Override
      public char readChar() throws IOException {
         return get();
      }

      @Override
      public int readInt() throws IOException {
         return get();
      }

      @Override
      public long readLong() throws IOException {
         return get();
      }

      @Override
      public float readFloat() throws IOException {
         return get();
      }

      @Override
      public double readDouble() throws IOException {
         return get();
      }

      @Override
      public String readLine() throws IOException {
         throw new UnsupportedOperationException();
      }

      @Override
      public String readUTF() throws IOException {
         return get();
      }

      @SuppressWarnings("unchecked")
      private <T> T get() throws IOException {
         try {
            return (T) marshaller.readObject(in);
         } catch(ClassNotFoundException e) {
            throw new MarshallingException(e);
         }
      }
   }
}
