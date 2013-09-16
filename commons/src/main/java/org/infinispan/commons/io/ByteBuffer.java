package org.infinispan.commons.io;

import java.io.InputStream;

/**
 * A byte buffer that exposes the internal byte array with minimal copying.
 * To be instantiated with {@link ByteBufferFactory}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface ByteBuffer {
   byte[] getBuf();

   int getOffset();

   int getLength();

   ByteBuffer copy();

   InputStream getStream();
}
