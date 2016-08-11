package org.infinispan.marshall.core.internal;

import org.infinispan.commands.RemoteCommandsFactory;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ExposedByteArrayOutputStream;
import org.infinispan.commons.logging.Log;
import org.infinispan.commons.logging.LogFactory;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.SerializeWith;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.factories.GlobalComponentRegistry;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.io.Serializable;

// TODO: If wrapped around GlobalMarshaller, there might not be need to implement StreamingMarshaller interface
// If exposed directly, e.g. not wrapped by GlobalMarshaller, it'd need to implement StreamingMarshaller
public final class InternalMarshaller implements StreamingMarshaller {

   private static final Log log = LogFactory.getLog(InternalMarshaller.class);
   private static final boolean trace = log.isTraceEnabled();

   final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();

   final Encoding<BytesObjectOutput, BytesObjectInput> enc = new BytesEncoding();
   final InternalExternalizerTable externalizers;

   // TODO: Default should be JBoss Marshaller (needs ExternalizerTable & GlobalConfiguration)
   // ^ External marshaller is what global configuration serialization config can tweak
   final StreamingMarshaller external = new InternalJavaSerialMarshaller();

   public InternalMarshaller(GlobalComponentRegistry gcr, RemoteCommandsFactory cmdFactory) {
      this.externalizers = new InternalExternalizerTable(enc, gcr, cmdFactory);
   }

   @Override
   public void start() {
      externalizers.start();
   }

   @Override
   public void stop() {
      externalizers.stop();
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      BufferSizePredictor sizePredictor = marshallableTypeHints.getBufferSizePredictor(obj);
      BytesObjectOutput out = new BytesObjectOutput(sizePredictor.nextSize(obj), this);
      AdvancedExternalizer<Object> ext = externalizers.findWriteExternalizer(obj, out);
      if (ext != null)
         ext.writeObject(out, obj);
      else
         external.objectToObjectStream(obj, out);

      sizePredictor.recordSize(out.bytes.length);
      return out.toBytes();
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      ObjectInput in = BytesObjectInput.from(buf, this);
      return objectFromObjectInput(in);
   }

   private Object objectFromObjectInput(ObjectInput in) throws IOException, ClassNotFoundException {
      AdvancedExternalizer<Object> ext = externalizers.findReadExternalizer(in);
      if (ext != null)
         return ext.readObject(in);
      else {
         try {
            return external.objectFromObjectStream(in);
         } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
         }
      }
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      BytesObjectOutput out = new BytesObjectOutput(estimatedSize, this);
      return new StreamBytesObjectOutput(os, out);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      out.writeObject(obj);
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
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      // Ignore length since boundary checks are not so useful here where the
      // unmarshalling code knows what to expect specifically. E.g. if reading
      // a byte[] subset within it, it's always appended with length.
      BytesObjectInput in = BytesObjectInput.from(buf, offset, this);
      return objectFromObjectInput(in);
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      // This is a very limited use case, e.g. reading from a JDBC ResultSet InputStream
      // So, this copying of the stream into a byte[] has not been problematic so far,
      // though it's not really ideal.
      int len = is.available();
      ExposedByteArrayOutputStream bytes;
      byte[] buf;
      if(len > 0) {
         bytes = new ExposedByteArrayOutputStream(len);
         buf = new byte[Math.min(len, 1024)];
      } else {
         // Some input stream providers do not implement available()
         bytes = new ExposedByteArrayOutputStream();
         buf = new byte[1024];
      }
      int bytesRead;
      while ((bytesRead = is.read(buf, 0, buf.length)) != -1) bytes.write(buf, 0, bytesRead);
      return objectFromByteBuffer(bytes.getRawBuffer(), 0, bytes.size());
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      Class<?> clazz = o.getClass();
      boolean containsMarshallable = marshallableTypeHints.isKnownMarshallable(clazz);
      if (containsMarshallable) {
         boolean marshallable = marshallableTypeHints.isMarshallable(clazz);
         if (trace)
            log.tracef("Marshallable type '%s' known and is marshallable=%b",
                  clazz.getName(), marshallable);

         return marshallable;
      } else {
         if (isMarshallableCandidate(o)) {
            boolean isMarshallable = true;
            try {
               objectToBuffer(o);
            } catch (Exception e) {
               isMarshallable = false;
               throw e;
            } finally {
               marshallableTypeHints.markMarshallable(clazz, isMarshallable);
            }
            return isMarshallable;
         }
         return false;
      }
   }

   private boolean isMarshallableCandidate(Object o) {
      return o instanceof Serializable
            || externalizers.isMarshallable(o)
            || o.getClass().getAnnotation(SerializeWith.class) != null;
            //|| o.getClass().getAnnotation(Externalize.class) != null;
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return marshallableTypeHints.getBufferSizePredictor(o.getClass());
   }

   //////

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      // TODO: Customise this generated block
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return new byte[0];  // TODO: Customise this generated block
   }

}
