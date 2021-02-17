package org.infinispan.query;

/**
 * Converts objects (cache keys only) to and from their Java types to String representations so that Infinispan can index them.
 * You need to convert custom types only.
 * Infinispan transforms boxed primitives, java.lang.String, java.util.UUID, and byte arrays internally.
 * <p>
 * Implementations must be thread-safe! It is recommended they are also stateless.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Transformer {

   /**
    * Transforms a String into an Object.
    *
    * @param str cannot be null
    * @return the Object that is encoded in the given String
    */
   Object fromString(String str);

   /**
    * Transforms an Object into a String.
    *
    * @param obj cannot be null
    * @return the String representation of the object
    */
   String toString(Object obj);
}
