package org.infinispan.persistence.keymappers;

/**
 * Defines the logic of mapping a key object to a String. This is required by certain cache stores, in order
 * to map each key to a String which the underlying store is capable of handling. It should generate a unique String
 * based on the supplied key.
 *
 * @author Mircea.Markus@jboss.com
 * @author Manik Surtani
 */
public interface Key2StringMapper {

   /**
    * Do we support this key type?
    * @param keyType type to test
    * @return true if the type is supported, false otherwise.
    */
   boolean isSupportedType(Class<?> keyType);

   /**
    * Must return an unique String for the supplied key.
    * @param key key to map to a String
    * @return String representation of the key
    */
   String getStringMapping(Object key);
}
