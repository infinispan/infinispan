package org.infinispan.commons.io;

/**
 * Used for building instances of {@link ByteBuffer}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface ByteBufferFactory {

   ByteBuffer newByteBuffer(byte[] b, int offset, int length);

}
