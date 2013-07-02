package org.infinispan.io;

/**
 * A byte buffer that exposes the internal byte array with minimal copying
 *
 * @author (various)
 * @since 4.0
 */
@Deprecated
public class ByteBuffer extends org.infinispan.commons.io.ByteBuffer {

   public ByteBuffer(byte[] buf, int offset, int length) {
      super(buf, offset, length);
   }
}
