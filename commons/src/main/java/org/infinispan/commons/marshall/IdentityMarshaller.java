package org.infinispan.commons.marshall;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;

/**
 * A marshaller that does not transform the content, only applicable to byte[] payloads.
 *
 * @since 9.3
 */
public class IdentityMarshaller extends AbstractMarshaller {

   public static final IdentityMarshaller INSTANCE = new IdentityMarshaller();

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
      byte[] payload = (byte[]) o;
      return new ByteBufferImpl(payload, 0, payload.length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
      return buf;
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o instanceof byte[];
   }

   @Override
   public MediaType mediaType() {
      return APPLICATION_UNKNOWN;
   }
}
