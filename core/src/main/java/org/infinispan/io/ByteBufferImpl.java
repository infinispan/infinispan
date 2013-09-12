package org.infinispan.io;

/**
 * A byte buffer that exposes the internal byte array with minimal copying
 *
 * @author (various)
 * @since 4.0
 */
@Deprecated
public class ByteBufferImpl extends org.infinispan.commons.io.ByteBufferImpl {

   public ByteBufferImpl(byte[] buf, int offset, int length) {
      super(buf, offset, length);
   }
}
