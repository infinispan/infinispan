package org.infinispan.marshaller.kryo;

import net.spy.memcached.CachedData;
import net.spy.memcached.transcoders.Transcoder;

/**
 * @author Ryan Emerson
 * @since 9.0
 */
public class KryoTranscoder implements Transcoder<Object> {

   private KryoMarshaller marshaller;

   public KryoTranscoder(KryoMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   public KryoMarshaller getMarshaller() {
      return marshaller;
   }

   public void setMarshaller(KryoMarshaller marshaller) {
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
