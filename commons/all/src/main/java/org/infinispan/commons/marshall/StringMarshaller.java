package org.infinispan.commons.marshall;

import java.io.IOException;
import java.nio.charset.Charset;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;

public class StringMarshaller extends AbstractMarshaller {

   final Charset charset;

   public StringMarshaller(Charset charset) {
      this.charset = charset;
   }

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
      byte[] bytes = o instanceof byte[] ? (byte[]) o : (o.toString()).getBytes(charset);
      return ByteBufferImpl.create(bytes);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) throws IOException, ClassNotFoundException {
      return new String(buf, charset);
   }

   @Override
   public boolean isMarshallable(Object o) {
      return o instanceof String;
   }

   @Override
   public MediaType mediaType() {
      return MediaType.TEXT_PLAIN;
   }

}
