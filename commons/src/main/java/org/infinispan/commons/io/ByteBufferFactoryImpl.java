package org.infinispan.commons.io;

/**
 * @author Mircea Markus
 * @since 6.0
 */
public class ByteBufferFactoryImpl implements ByteBufferFactory {

   @Override
   public ByteBuffer newByteBuffer(byte[] b, int offset, int length) {
      return new ByteBufferImpl(b, offset, length);
   }
}
