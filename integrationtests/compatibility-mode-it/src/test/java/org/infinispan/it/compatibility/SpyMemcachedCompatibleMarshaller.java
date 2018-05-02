package org.infinispan.it.compatibility;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.io.ByteBuffer;
import org.infinispan.commons.io.ByteBufferImpl;
import org.infinispan.commons.marshall.AbstractMarshaller;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.SerializingTranscoder;
import net.spy.memcached.transcoders.Transcoder;


public class SpyMemcachedCompatibleMarshaller extends AbstractMarshaller {

   private final Transcoder<Object> transcoder = new SerializingTranscoder();

   @Override
   protected ByteBuffer objectToBuffer(Object o, int estimatedSize) {
      CachedData encoded = transcoder.encode(o);
      return new ByteBufferImpl(encoded.getData(), 0, encoded.getData().length);
   }

   @Override
   public Object objectFromByteBuffer(byte[] buf, int offset, int length) {
      return transcoder.decode(new CachedData(0, buf, length));
   }

   @Override
   public boolean isMarshallable(Object o) throws Exception {
      try {
         transcoder.encode(o);
         return true;
      } catch (Throwable t) {
         return false;
      }
   }

   @Override
   public MediaType mediaType() {
      return MediaType.parse("application/x-memcached");
   }

}
