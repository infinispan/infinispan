package org.infinispan.commons.marshall;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_UNKNOWN;

import java.util.Arrays;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;

/**
 * A marshaller that does not transform the content, only applicable to byte[] payloads.
 *
 * <p>
 * Reading a whole array returns that same array. Reading part of an array returns a copy of that part.
 * </p>
 *
 * @since 9.3
 */
public class IdentityMarshaller extends AbstractMarshaller {

   public static final IdentityMarshaller INSTANCE = new IdentityMarshaller();

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
      return ByteBufferImpl.create((byte[]) o);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
      if (offset == 0 && length == buf.length)
         return buf;
      return Arrays.copyOfRange(buf, offset, offset + length);
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
