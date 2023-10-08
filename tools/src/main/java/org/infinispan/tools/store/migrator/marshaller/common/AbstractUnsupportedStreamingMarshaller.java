package org.infinispan.tools.store.migrator.marshaller.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.marshall.AbstractMarshaller;
import org.infinispan.jboss.marshalling.commons.StreamingMarshaller;

/**
 * An implementation of {@link AbstractMarshaller} that throws {@link UnsupportedOperationException} for all methods.
 *
 * @author Ryan Emerson
 * @since 10.0
 */
abstract public class AbstractUnsupportedStreamingMarshaller extends AbstractMarshaller implements StreamingMarshaller {

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) throws IOException, InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public ObjectOutput startObjectOutput(OutputStream os, boolean isReentrant, int estimatedSize) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public void finishObjectOutput(ObjectOutput oo) {
      throw new UnsupportedOperationException();
   }

   @Override
   public void objectToObjectStream(Object obj, ObjectOutput out) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public ObjectInput startObjectInput(InputStream is, boolean isReentrant) throws IOException {
      throw new UnsupportedOperationException();
   }

   @Override
   public void finishObjectInput(ObjectInput oi) {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object objectFromObjectStream(ObjectInput in) throws IOException, ClassNotFoundException, InterruptedException {
      throw new UnsupportedOperationException();
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      throw new UnsupportedOperationException();
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      throw new UnsupportedOperationException();
   }

   @Override
   public MediaType mediaType() {
      throw new UnsupportedOperationException();
   }
}
