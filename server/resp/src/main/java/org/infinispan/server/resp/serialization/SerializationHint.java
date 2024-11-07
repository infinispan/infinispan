package org.infinispan.server.resp.serialization;

/**
 * Hints the serializer for nested object's type.
 *
 * <p>
 * Nested objects, such as arrays and maps, are nested with many objects. The hint aids the serializer in identifying the
 * type of elements inside the structure. This approach avoids traversing the complete list of serializers in the system.
 * </p>
 *
 * <p>
 * The hints only work for 1-level nested objects. Deeply nested or heterogeneous structures utilize an unknown hint and
 * search the registry for a match. There is a distinction between {@link SimpleHint} and {@link KeyValueHint}.
 * </p>
 *
 * @author José Bolina
 */
public interface SerializationHint {

   /**
    * Provide hints of simple primitive elements.
    */
   interface SimpleHint extends SerializationHint {

      /**
       * Identify the underlying RESP3 type.
       */
      void serialize(Object object, ResponseWriter writer);
   }

   /**
    * Provide hints of types for key-value structures.
    *
    * <p>
    * Composes two {@link SimpleHint} to identify the key and value types. Therefore, key and value types must be homogeneous.
    * </p>
    *
    * @param key The type of keys in the structure.
    * @param value The type of values in the structure.
    */
   record KeyValueHint(SimpleHint key, SimpleHint value) implements SerializationHint { }
}
