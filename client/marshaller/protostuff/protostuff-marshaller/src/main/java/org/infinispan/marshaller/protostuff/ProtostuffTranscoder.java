package org.infinispan.marshaller.protostuff;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class ProtostuffTranscoder implements Transcoder<Object> {

   private ProtostuffMarshaller marshaller;

   public ProtostuffTranscoder(ProtostuffMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   public ProtostuffMarshaller getMarshaller() {
      return marshaller;
   }

   public void setMarshaller(ProtostuffMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public boolean asyncDecode(CachedData d) {
      return false;
   }

   @Override
   public CachedData encode(Object o) {
      try {
         byte[] bytes = marshaller.objectToByteBuffer(o);
         return new CachedData(0, bytes, bytes.length);
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   @Override
   public Object decode(CachedData d) {
      byte[] data = d.getData();
      try {
         return marshaller.objectFromByteBuffer(data);
      } catch (Throwable t) {
         throw new RuntimeException(t);
      }
   }

   @Override
   public int getMaxSize() {
      return Integer.MAX_VALUE;
   }
}
