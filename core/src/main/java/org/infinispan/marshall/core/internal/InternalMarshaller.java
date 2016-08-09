package org.infinispan.marshall.core.internal;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.MarshallableTypeHints;
import org.infinispan.commons.marshall.StreamingMarshaller;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

// TODO: If wrapped around GlobalMarshaller, there might not be need to implement StreamingMarshaller interface
// If exposed directly, e.g. not wrapped by GlobalMarshaller, it'd need to implement StreamingMarshaller
public final class InternalMarshaller implements StreamingMarshaller {

   final MarshallableTypeHints marshallableTypeHints = new MarshallableTypeHints();
   final InternalExternalizerTable externalizers = new InternalExternalizerTable();

   final Encoding<ByteArrayObjectOutput, ByteArrayObjectInput> enc = new ByteArrayEncoding();

   // TODO: Default should be JBoss Marshaller (needs ExternalizerTable & GlobalConfiguration)
   // ^ External marshaller is what global configuration serialization config can tweak
   final StreamingMarshaller external = null;

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
      int estimatedSize = getEstimatedSize(obj);
      ByteArrayObjectOutput oo = new ByteArrayObjectOutput(estimatedSize, this);
      AdvancedExternalizer<Object> ext = externalizers.findWriteExternalizer(obj, oo);
      if (ext != null) {
         ext.writeObject(oo, obj);
         return oo.toBytes();
      }

      // TODO: Delegate to external marshaller
      // TODO: Add finally section to update size estimates
      return null;
   }

   private int getEstimatedSize(Object obj) {
      return obj != null
            ? marshallableTypeHints.getBufferSizePredictor(obj.getClass()).nextSize(obj)
            : 1;
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      ObjectInput in = new ByteArrayObjectInput(buf, this);
      AdvancedExternalizer<Object> ext = externalizers.findReadExternalizer(in);
      if (ext != null)
         return ext.readObject(in);

      // TODO: Delegate to external marshaller
      return null;
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
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
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
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return null;  // TODO: Customise this generated block
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
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

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
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
