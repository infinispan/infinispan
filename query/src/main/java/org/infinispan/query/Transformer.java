package org.infinispan.query;

/**
 * Convert objects (cache keys only) from their original Java types to a String representation (which is suitable to be
 * used in a Lucene index) and vice versa. Transformers are needed only for custom types. Primitive types (boxed),
 * java.lang.String and byte arrays are internally handled without the need of a user-supplied Transformer.
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
