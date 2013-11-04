package org.infinispan.commons.io;

import java.io.InputStream;

/**
 * A byte buffer that exposes the internal byte array with minimal copying. To be instantiated with {@link
 * ByteBufferFactory}.
 *
 * @author Mircea Markus
 * @since 6.0
 */
public interface ByteBuffer {
   /**
    * Returns the underlying buffer.
    */
   byte[] getBuf();

   /**
    * Returns the offset within the underlying byte[] (as returned by {@link #getBuf()} owned by this buffer instance.
    */
   int getOffset();

   /**
    * Length bytes, starting from offset, within the underlying byte[] (as returned by {@link #getBuf()} are owned
    * by this buffer instance.
    *
    */
   int getLength();

   /**
    * Returns a new byte[] instance of size {@link #getLength()} that contains all the bytes owned by this buffer.
    */
   ByteBuffer copy();
}
