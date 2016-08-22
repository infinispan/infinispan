package org.infinispan.marshall.core.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.StreamingMarshaller;

/**
 * Java serialization marshaller for unknown types.
 * Unknown types are those types for which there are no externalizers.
 */
public final class ExternalJavaMarshaller implements StreamingMarshaller {

   final ExternalMarshallerWhiteList whiteList = new ExternalMarshallerWhiteList();

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      whiteList.checkWhiteListed(obj);

      DelegateOutputStream stream = new DelegateOutputStream(out);
      try (ObjectOutputStream objectStream = new ObjectOutputStream(stream)) {
         objectStream.writeObject(obj);
      }
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      DelegateInputStream stream = new DelegateInputStream(in);
      try (ObjectInputStream objectStream = new ObjectInputStream(stream)) {
         return objectStream.readObject();
      }
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      throw new RuntimeException("NYI");
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      throw new RuntimeException("NYI");
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      throw new RuntimeException("NYI");
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      throw new RuntimeException("NYI");
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      throw new RuntimeException("NYI");
   }

   @Override
   public void stop() {
      throw new RuntimeException("NYI");
   }

   @Override
   public void start() {
      throw new RuntimeException("NYI");
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      throw new RuntimeException("NYI");
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      throw new RuntimeException("NYI");
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      throw new RuntimeException("NYI");
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      throw new RuntimeException("NYI");
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      throw new RuntimeException("NYI");
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      throw new RuntimeException("NYI");
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      throw new RuntimeException("NYI");
   }

   final static class DelegateOutputStream extends OutputStream {

      final ObjectOutput out;

      DelegateOutputStream(ObjectOutput out) {
         this.out = out;
      }

      @Override
      public void write(int b) throws IOException {
         out.writeByte(b);
      }

   }

   final static class DelegateInputStream extends InputStream {

      final ObjectInput in;

      DelegateInputStream(ObjectInput in) {
         this.in = in;
      }

      @Override
      public int read() throws IOException {
         return in.readUnsignedByte();
      }

   }

}
