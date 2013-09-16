package org.infinispan.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.StreamingMarshaller;

/**
 * LegacyStreamingMarshallerAdapter.
 *
 * @author Tristan Tarrant
 * @since 6.0
 */
@Deprecated
public class LegacyStreamingMarshallerAdapter implements StreamingMarshaller {
   private final org.infinispan.marshall.StreamingMarshaller delegate;

   public LegacyStreamingMarshallerAdapter(org.infinispan.marshall.StreamingMarshaller delegate) {
      this.delegate = delegate;
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      return delegate.startObjectOutput(os, isReentrant, estimatedSize);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      return delegate.objectToByteBuffer(obj, estimatedSize);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return delegate.objectToByteBuffer(obj);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      return delegate.objectFromByteBuffer(buf);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return delegate.objectFromByteBuffer(buf, offset, length);
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      delegate.finishObjectOutput(oo);
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      delegate.objectToObjectStream(obj, out);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return delegate.objectToBuffer(o);
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      return delegate.startObjectInput(is, isReentrant);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return delegate.isMarshallable(o);
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return new LegacyBufferSizePredictorAdapter(delegate.getBufferSizePredictor(o));
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      delegate.finishObjectInput(oi);
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      return delegate.objectFromObjectStream(in);
   }

   @Override
   public Object objectFromInputStream(InputStream is) throws IOException, ClassNotFoundException {
      return delegate.objectFromInputStream(is);
   }

   @Override
   public void stop() {
      delegate.stop();
   }

   @Override
   public void start() {
      delegate.start();
   }
}
