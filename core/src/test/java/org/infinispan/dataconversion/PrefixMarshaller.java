package org.infinispan.dataconversion;

import java.io.IOException;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.BufferSizePredictor;
import org.infinispan.commons.marshall.JavaSerializationMarshaller;
import org.infinispan.commons.marshall.Marshaller;

/**
 * A test marshaller that wraps JavaSerializationMarshaller and adds a prefix byte to distinguish it.
 *
 * @author William Burns
 * @since 16.2
 */
public class PrefixMarshaller implements Marshaller {

   private final byte prefix;
   private final JavaSerializationMarshaller delegate = new JavaSerializationMarshaller();

   public PrefixMarshaller(byte prefix) {
      this.prefix = prefix;
   }

   public PrefixMarshaller() {
      this((byte) 42); // Default prefix
   }

   @Override
   public void initialize(ClassAllowList allowList) {
      delegate.initialize(allowList);
   }

   @Override
   public byte[] objectToByteBuffer(Object obj, int estimatedSize) throws IOException, InterruptedException {
      byte[] delegateBytes = delegate.objectToByteBuffer(obj, estimatedSize);
      byte[] result = new byte[delegateBytes.length + 1];
      result[0] = prefix;
      System.arraycopy(delegateBytes, 0, result, 1, delegateBytes.length);
      return result;
   }

   @Override
   public byte[] objectToByteBuffer(Object obj) throws IOException, InterruptedException {
      return objectToByteBuffer(obj, 512);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf) throws IOException, ClassNotFoundException {
      if (buf.length == 0 || buf[0] != prefix) {
         throw new IOException("Invalid prefix byte, expected " + prefix + " but got " + (buf.length > 0 ? buf[0] : "empty"));
      }
      byte[] delegateBytes = new byte[buf.length - 1];
      System.arraycopy(buf, 1, delegateBytes, 0, buf.length - 1);
      return delegate.objectFromByteBuffer(delegateBytes);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      byte[] copy = new byte[length];
      System.arraycopy(buf, offset, copy, 0, length);
      return objectFromByteBuffer(copy);
   }

   @Override
   public ByteBuffer objectToBuffer(Object o) throws IOException, InterruptedException {
      return ByteBufferImpl.create(objectToByteBuffer(o));
   }

   @Override
   public BufferSizePredictor getBufferSizePredictor(Object o) {
      return delegate.getBufferSizePredictor(o);
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      return delegate.isMarshallable(o);
   }

   @Override
   public MediaType mediaType() {
      return delegate.mediaType();
   }
}
