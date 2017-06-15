package org.infinispan.commons.dataconversion;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;
import org.infinispan.commons.marshall.jboss.GenericJBossMarshaller;

/**
 * Encoder that read/write marshalled content and store them unmarshalled.
 *
 * @since 9.1
 */
public class CompatModeEncoder implements Encoder {

   protected final Marshaller marshaller;

   public CompatModeEncoder(Marshaller marshaller) {
      this.marshaller = marshaller == null ? new GenericJBossMarshaller() : marshaller;
   }

   public Object toStorage(Object content) {
      if (content instanceof byte[]) {
         try {
            return unmarshall((byte[]) content);
         } catch (IOException | ClassNotFoundException e) {
            throw new CacheException(e);
         }
      }
      return content;
   }

   public Object fromStorage(Object content) {
      if (content == null) return null;
      try {
         return marshall(content);
      } catch (InterruptedException | IOException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return true;
   }

   protected Object unmarshall(byte[] source) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(source);
   }

   protected byte[] marshall(Object source) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(source);
   }
}
