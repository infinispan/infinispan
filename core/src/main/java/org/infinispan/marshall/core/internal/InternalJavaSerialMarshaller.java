package org.infinispan.marshall.core.internal;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.StreamingMarshaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;

/**
 * Java serialization marshaller for unknown types.
 * Unknown types are those types for which there are no externalizers.
 */
final class InternalJavaSerialMarshaller implements StreamingMarshaller {

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      // TODO: Remove temporary check once all types are covered
      // Temporary check to verify that no java.* nor org.infinispan.*
      // instances for which externalizer should have been created get through
      // this method
      String pkg = obj.getClass().getPackage().getName();
      if (obj instanceof Serializable
            && (pkg.startsWith("java.") || pkg.startsWith("org.infinispan.") || pkg.startsWith("org.jgroups."))
            && !isWhiteList(obj.getClass().getName())) {
         throw new RuntimeException("Check support for: " + obj.getClass());
      }

      DelegateOutputStream stream = new DelegateOutputStream(out);
      try (ObjectOutputStream objectStream = new ObjectOutputStream(stream)) {
         objectStream.writeObject(obj);
      }
   }

   private boolean isWhiteList(String className) {
      return className.equals("org.infinispan.marshall.core.JBossMarshallingTest$ObjectThatContainsACustomReadObjectMethod")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Child1")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Child2")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Human")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$HumanComparator")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$Pojo")
            || className.equals("org.infinispan.marshall.VersionAwareMarshallerTest$PojoWhichFailsOnUnmarshalling")
            || className.equals("org.infinispan.persistence.BaseStoreFunctionalTest$Pojo")
            || className.equals("org.infinispan.test.data.Person")
            || className.equals("org.infinispan.util.concurrent.TimeoutException")
            ;
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
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      // TODO: Customise this generated block
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      // TODO: Customise this generated block
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void stop() {
      // TODO: Customise this generated block
   }

   @Override
   public void start() {
      // TODO: Customise this generated block
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return new byte[0];  // TODO: Customise this generated block
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return new byte[0];  // TODO: Customise this generated block
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return false;  // TODO: Customise this generated block
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return null;  // TODO: Customise this generated block
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
