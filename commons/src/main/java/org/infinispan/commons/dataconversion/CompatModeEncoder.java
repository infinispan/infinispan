package org.infinispan.commons.dataconversion;

import static org.infinispan.commons.dataconversion.EncoderIds.COMPAT;

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

   @Override
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

   @Override
   public Object fromStorage(Object content) {
      try {
         return marshall(content);
      } catch (Exception e) {
         throw new CacheException(e);
      }
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return true;
   }

   @Override
   public short id() {
      return COMPAT;
   }

   protected Object unmarshall(byte[] source) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(source);
   }

   protected Object marshall(Object source) throws Exception {
      if (marshaller.isMarshallable(source))
         return marshaller.objectToByteBuffer(source);
      return source;
   }
}
