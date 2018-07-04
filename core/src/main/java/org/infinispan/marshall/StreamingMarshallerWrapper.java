package org.infinispan.marshall;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.StreamingMarshaller;

/**
 * A wrapper {@link StreamingMarshaller} implementation for {@link Marshaller} instances that throws {@link UnsupportedOperationException}
 * for all methods that are not in the {@link Marshaller} interface.
 * // TODO
 * @author remerson@redhat.com
 * @since 9.4
 */
public class StreamingMarshallerWrapper implements StreamingMarshaller {

   private final Marshaller delegate;

   public StreamingMarshallerWrapper(Marshaller delegate) {
      this.delegate = delegate;
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) {
      throw new UnsupportedOperationException();
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object objectFromInputStream(InputStream is) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void stop() {
      // no-op
   }

   @Override
   public void start() {
      // no-op
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
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return delegate.objectToBuffer(o);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return delegate.isMarshallable(o);
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return delegate.getBufferSizePredictor(o);
   }

   @Override
   public MediaType mediaType() {
      return delegate.mediaType();
   }
}
