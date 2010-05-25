package org.infinispan.loaders.remote;

import org.infinispan.client.hotrod.HotRodMarshaller;
import org.infinispan.client.hotrod.exceptions.HotRodClientException;
import org.infinispan.marshall.Marshaller;

import java.io.IOException;

/**
 * Marshaller used internally by the remote cache store.
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class InternalCacheEntryMarshaller implements HotRodMarshaller {

   public final Marshaller marshaller;

   public InternalCacheEntryMarshaller(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public byte[] marshallObject(Object toMarshall) {
      try {
         return marshaller.objectToByteBuffer(toMarshall);
      } catch (IOException e) {
         throw new HotRodClientException(e);
      }
   }

   @Override
   public Object readObject(byte[] bytes) {
      try {
         return marshaller.objectFromByteBuffer(bytes);
      } catch (Exception e) {
         throw new HotRodClientException(e);
      }
   }
}
