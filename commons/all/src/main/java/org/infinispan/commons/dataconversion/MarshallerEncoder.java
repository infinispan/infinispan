package org.infinispan.commons.dataconversion;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.Marshaller;

/**
 * Encoder that uses a {@link Marshaller} to convert objects to byte[] and back.
 *
 * @since 9.1
 * @deprecated Since 12.1, to be removed in a future version.
 */
@Deprecated(forRemoval=true, since = "12.1")
public abstract class MarshallerEncoder implements Encoder {

   private final Marshaller marshaller;

   public MarshallerEncoder(Marshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public Object toStorage(Object content) {
      try {
         return marshall(content);
      } catch (IOException | InterruptedException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public Object fromStorage(Object stored) {
      try {
         return stored instanceof byte[] ? marshaller.objectFromByteBuffer((byte[]) stored) : stored;
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   protected Object unmarshall(byte[] source) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(source);
   }

   protected byte[] marshall(Object source) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(source);
   }
}
