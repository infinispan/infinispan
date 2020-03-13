package org.infinispan.commons.dataconversion;

import java.io.IOException;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.StreamingMarshaller;
import org.infinispan.commons.marshall.WrappedByteArray;

/**
 * Encoder for StoreType.BINARY. For String and primitives, store unencoded. For other objects, store them marshalled.
 *
 * @since 9.1
 */
public class BinaryEncoder implements Encoder {

   private final StreamingMarshaller marshaller;

   public BinaryEncoder(StreamingMarshaller marshaller) {
      this.marshaller = marshaller;
   }

   @Override
   public Object toStorage(Object content) {
      try {
         return skipEncoding(content) ? content : marshall(content);
      } catch (IOException | InterruptedException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public Object fromStorage(Object stored) {
      try {
         if (isTypeExcluded(stored.getClass())) {
            return stored;
         }
         return stored instanceof byte[] ? marshaller.objectFromByteBuffer((byte[]) stored) : stored;
      } catch (IOException | ClassNotFoundException e) {
         throw new CacheException(e);
      }
   }

   @Override
   public boolean isStorageFormatFilterable() {
      return false;
   }

   @Override
   public MediaType getStorageFormat() {
      return MediaType.APPLICATION_INFINISPAN_BINARY;
   }

   @Override
   public short id() {
      return EncoderIds.BINARY;
   }

   private static boolean isTypeExcluded(Class<?> type) {
      return type.equals(String.class) || type.isPrimitive() ||
            type.equals(Boolean.class) || type.equals(Character.class) ||
            type.equals(Byte.class) || type.equals(Short.class) || type.equals(Integer.class) ||
            type.equals(Long.class) || type.equals(Float.class) || type.equals(Double.class) ||
            // We cannot exclude array as we can't tell the difference between byte[] and WrappedByteArray
            type.equals(WrappedByteArray.class);
   }

   private boolean skipEncoding(Object source) {
      return isTypeExcluded(source.getClass());
   }

   protected Object unmarshall(byte[] source) throws IOException, ClassNotFoundException {
      return marshaller.objectFromByteBuffer(source);
   }

   protected byte[] marshall(Object source) throws IOException, InterruptedException {
      return marshaller.objectToByteBuffer(source);
   }

}
