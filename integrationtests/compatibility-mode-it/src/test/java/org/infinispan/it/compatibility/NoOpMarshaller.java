package org.infinispan.it.compatibility;

import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;

/**
 * Marshaller that writes and reads raw bytes only. Simulates a client that only reads and writes opaque byte[].
 *
 * @since 9.2
 */
public class NoOpMarshaller extends AbstractMarshaller {

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
      if (!(o instanceof byte[])) throw new IllegalArgumentException("Content must be byte[]");
      byte[] bytes = (byte[]) o;
      return new ByteBufferImpl(bytes, 0, bytes.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
      return buf;
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o instanceof byte[];
   }
}
