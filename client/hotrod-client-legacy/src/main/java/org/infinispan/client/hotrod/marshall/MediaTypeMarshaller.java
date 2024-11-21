package org.infinispan.client.hotrod.marshall;

import org.infinispan.commons.configuration.ClassAllowList;
import org.infinispan.commons.dataconversion.MediaType;

/**
 * A marshaller that takes the {@link MediaType} decision to marshall/unmarshall.
 *
 * @since 15.0
 */
public interface MediaTypeMarshaller {

   /**
    * The decided type for the key.
    *
    * @return The media type the marshaller uses for keys.
    */
   MediaType getKeyType();

   /**
    * The decided type for the values.
    *
    * @return The media type the marshaller uses for values.
    */
   MediaType getValueType();

   /**
    * Transforms the key object to the marshalled form.
    * The marshalled format respects the define media type for the key.
    *
    * @param key: Key object to marshall.
    * @return The byte array representation of the object in the decided key media type.
    */

   byte[] keyToBytes(Object key);

   /**
    * Transforms the value object to the marshalled form.
    * The marshalled format respects the define media type for the value.
    *
    * @param value: Value object to marshall.
    * @return The byte array representation of the object in the decided value media type.
    */
   byte[] valueToBytes(Object value);

   /**
    * Unmarshall the byte array into the object form.
    * The unmarshalling follows the process by the decided key media type.
    *
    * @param bytes: Byte array to unmarshall.
    * @param allowList: The allowed list of classes to use during unmarshalling.
    * @return The object store in the byte array representation.
    */
   <T> T bytesToKey(byte[] bytes, ClassAllowList allowList);

   /**
    * Unmarshall the byte array into the object form.
    * The unmarshalling follows the process by the decided value media type.
    *
    * @param bytes: Byte array to unmarshall.
    * @param allowList: The allowed list of classes to use during unmarshalling.
    * @return The object store in the byte array representation.
    */
   <T> T bytesToValue(byte[] bytes, ClassAllowList allowList);
}
