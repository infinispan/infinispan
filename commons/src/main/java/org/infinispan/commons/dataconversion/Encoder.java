package org.infinispan.commons.dataconversion;

/**
 * Used to convert data between read/write format and storage format.
 *
 * @since 9.1
 */
public interface Encoder {

   /**
    * Convert data in the read/write format to the storage format.
    *
    * @param content data to be converted.
    * @return Object in the storage format.
    */
   Object toStorage(Object content);

   /**
    * Convert from storage format to the read/write format.
    *
    * @param content data as stored in the cache.
    * @return data in the read/write format
    */
   Object fromStorage(Object content);

   /**
    * @return if true, will perform stream and related operation in the storage format.
    */
   boolean isStorageFormatFilterable();

}
