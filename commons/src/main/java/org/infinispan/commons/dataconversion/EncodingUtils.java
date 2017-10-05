package org.infinispan.commons.dataconversion;

/**
 * Utilities to encode/decode keys and values from caches.
 *
 * @since 9.1
 * @deprecated Use the org.infinispan.encoding.DataConversion obtained from the AdvancedCache.
 */
public final class EncodingUtils {

   private EncodingUtils() {
   }

   /**
    * Decode object from storage format.
    *
    * @param stored  Object in the storage format.
    * @param encoder the {@link Encoder} used for data conversion.
    * @param wrapper the {@link Wrapper} used to decorate the converted data.
    * @return Object decoded and unwrapped.
    */
   public static Object fromStorage(Object stored, Encoder encoder, Wrapper wrapper) {
      if (encoder == null || wrapper == null) {
         throw new IllegalArgumentException("Both Encoder and Wrapper must be provided!");
      }
      if (stored == null) return null;
      return encoder.fromStorage(wrapper.unwrap(stored));
   }

   /**
    * Encode object to storage format.
    *
    * @param toStore  Object to be encoded.
    * @param encoder the {@link Encoder} used for data conversion.
    * @param wrapper the {@link Wrapper} used to decorate the converted data.
    * @return Object decoded and unwrapped.
    */
   public static Object toStorage(Object toStore, Encoder encoder, Wrapper wrapper) {
      if (encoder == null || wrapper == null) {
         throw new IllegalArgumentException("Both Encoder and Wrapper must be provided!");
      }
      if (toStore == null) return null;
      return wrapper.wrap(encoder.toStorage(toStore));
   }
}
