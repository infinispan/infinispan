package org.infinispan.marshall.core.internal;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.configuration.global.GlobalConfiguration;
import org.infinispan.marshall.core.JBossMarshaller;
import org.jboss.marshalling.ByteInput;
import org.jboss.marshalling.ByteOutput;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

final class ExternalJBossMarshaller implements StreamingMarshaller {

   final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();
   final ExternalMarshallerWhiteList whiteList = new ExternalMarshallerWhiteList();
   final JBossMarshaller marshaller;

   public ExternalJBossMarshaller(InternalExternalizerTable externalizers, GlobalConfiguration globalCfg) {
      this.marshaller = new InternalJBossMarshaller(externalizers, globalCfg);
      //this.marshaller = new JBossMarshaller(null, globalCfg);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      whiteList.checkWhiteListed(obj);

      if (out instanceof PositionalBuffer.Output) {
         PositionalBuffer.Output posOut = (PositionalBuffer.Output) out;

         BufferSizePredictor sizePredictor = marshallableTypeHints
               .getBufferSizePredictor(obj.getClass());
         int estimatedSize = sizePredictor.nextSize(obj);

         int beforePos = posOut.savePosition();

         ObjectOutput jbossOut = marshaller.startObjectOutput(
               new JBossByteOutput(out), false, estimatedSize);
         try {
            jbossOut.writeObject(obj);
         } finally {
            marshaller.finishObjectOutput(jbossOut);
         }

         int afterPos = posOut.writePosition(beforePos);
         sizePredictor.recordSize(afterPos - beforePos);
      } else {
         throw new IOException("External JBoss Marshalling requires position tracking object output");
      }
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException {
      if (in instanceof PositionalBuffer.Input) {
         PositionalBuffer.Input posIn = (PositionalBuffer.Input) in;

         int pos = in.readInt();
         ObjectInput jbossIn = marshaller.startObjectInput(new JBossByteInput(in), false);
         try {
            return jbossIn.readObject();
         } finally {
            marshaller.finishObjectInput(jbossIn);
            posIn.rewindPosition(pos);
         }
      } else {
         throw new IOException("External JBoss Marshalling requires position rewinding object output");
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
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      throw new RuntimeException("NYI");
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
         try {
            return in.readUnsignedByte();
         } catch (ArrayIndexOutOfBoundsException e) {
            // When JBoss Marshalling starts reading a stream, it buffers
            // the contents, so it'll try as read as much as it can, so it
            // can end up reading too far, in which case launder the exception
            // into an IOException so that reading can stop without causing
            // issues.
            throw new EOFException();
         }
      }

   }

   static final class InternalJBossMarshaller extends JBossMarshaller {
      final InternalExternalizerTable externalizers;

      InternalJBossMarshaller(InternalExternalizerTable externalizers, GlobalConfiguration globalCfg) {
         super(null, globalCfg);
         this.externalizers = externalizers;
      }

      @Override
      public void start() {
         super.start();
         baseCfg.setObjectTable(new ObjectTable() {
            @Override
            public Writer getObjectWriter(Object object) throws IOException {
               AdvancedExternalizer<Object> ext = (AdvancedExternalizer<Object>)
                     externalizers.writers.get(object.getClass());
               if (ext != null)
                  return (m, obj) -> {
                     m.writeInt(ext.getId());
                     ext.writeObject(m, obj);
                  };

               return null;
            }

            @Override
            public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
               int id = unmarshaller.readInt();
               AdvancedExternalizer<?> ext = externalizers.readers.get(id);
               if (ext == null) {
                  // Try as foreign externalizer
                  int foreignId = externalizers.generateForeignReaderIndex(id);
                  ext = externalizers.readers.get(foreignId);
               }

               return ext.readObject(unmarshaller);
            }
         });
      }
   }

//   static final class JBossObjectOutput implements ObjectOutput {
//
//   }

//   final static class InternalObjectTable implements ObjectTable {
//
//      @Override
//      public Writer getObjectWriter(Object object) throws IOException {
//         return null;  // TODO: Customise this generated block
//      }
//
//      @Override
//      public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
//         return null;  // TODO: Customise this generated block
//      }
//
//   }

}