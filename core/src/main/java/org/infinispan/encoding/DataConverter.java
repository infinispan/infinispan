package org.infinispan.encoding;

/**
 * Interface for converting data between storage and request formats.
 *
 * @since 16.2
 */
public interface DataConverter {

   /**
    * Converts from storage format to request format.
    *
    * @param stored the object in storage format
    * @return the object in request format
    */
   Object fromStorage(Object stored);

   /**
    * Converts from request format to storage format.
    *
    * @param toStore the object in request format
    * @return the object in storage format
    */
   Object toStorage(Object toStore);
}
