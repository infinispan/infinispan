package org.infinispan.marshall.core;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.jboss.ExtendedRiverUnmarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.jboss.marshalling.ByteInput;
import org.jboss.marshalling.ByteOutput;
import org.jboss.marshalling.ObjectTable;

final class ExternalJBossMarshaller implements StreamingMarshaller {

   final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();
   final JBossMarshaller marshaller;

   ExternalJBossMarshaller(ObjectTable objectTable, GlobalConfiguration globalCfg) {
      this.marshaller = new JBossMarshaller(objectTable, globalCfg);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      BufferSizePredictor sizePredictor = marshallableTypeHints
            .getBufferSizePredictor(obj.getClass());
      int estimatedSize = sizePredictor.nextSize(obj);
      ObjectOutput jbossOut = marshaller.startObjectOutput(
            new JBossByteOutput(out), false, estimatedSize);
      try {
         jbossOut.writeObject(obj);
      } finally {
         marshaller.finishObjectOutput(jbossOut);
      }
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      ExtendedRiverUnmarshaller jbossIn = (ExtendedRiverUnmarshaller)
            marshaller.startObjectInput(new JBossByteInput(in), false);
      try {
         return jbossIn.readObject();
      } finally {
         // Rewind by skipping backwards (negatively)
         in.skipBytes(-jbossIn.getUnreadBufferedCount());
         marshaller.finishObjectInput(jbossIn);
      }
   }

   @Override
   public void start() {
      marshaller.start();
   }

   @Override
   public void stop() {
      marshaller.stop();
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return marshaller.isMarshallable(o);
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      throw new UnsupportedOperationException("No longer in use");
   }

   @Override
   public MediaType mediaType() {
      return MediaType.APPLICATION_JBOSS_MARSHALLING;
   }

   static final class JBossByteOutput extends OutputStream implements ByteOutput {

      final ObjectOutput out;

      JBossByteOutput(ObjectOutput out) {
         this.out = out;
      }

      @Override
      public void write(int b) throws IOException {
         out.write(b);
      }

   }

   static final class JBossByteInput extends InputStream implements ByteInput {

      final ObjectInput in;

      JBossByteInput(ObjectInput in) {
         this.in = in;
      }

      @Override
      public int read() throws IOException {
         return in.read();
      }

      @Override
      public int read(byte[] b, int off, int len) throws IOException {
         return in.read(b, off, len);
      }
   }

}
