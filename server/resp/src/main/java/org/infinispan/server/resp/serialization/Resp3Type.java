package org.infinispan.server.resp.serialization;

import org.infinispan.server.resp.ByteBufPool;

/**
 * Identifies the RESP3 type correspondent of a Java object.
 *
 * <p>
 * This enumeration includes a subset of the types available in RESP3, including unknown types. The types help to identify
 * the RESP3 correspondent type for nested data structures, such as lists and maps. The hints only work for 1-level nested
 * objects. Deeply nested or heterogeneous structures utilize an unknown hint and search the registry for a match.
 * </p>
 *
 * <p>
 * The type only covers the primitive types in RESP. We do not include nested objects here. If a nested object is needed,
 * utilize the appropriate method in {@link Resp3Response} and provide a {@link JavaObjectSerializer} to serialize.
 * </p>
 *
 * @author José Bolina
 * @see <a href="https://github.com/antirez/RESP3/blob/master/spec.md#resp3-types">RESP3 Types</a>
 */
public enum Resp3Type implements SerializationHint.SimpleHint {
   /**
    * Strings which do not contain any of the escape characters ('\n' or '\r').
    *
    * @see Resp3Response#simpleString(CharSequence, ByteBufPool)
    */
   SIMPLE_STRING {
      @Override
      public void serialize(Object object, ByteBufPool alloc) {
         Resp3Response.simpleString((CharSequence) object, alloc);
      }
   },

   /**
    * Any type of string.
    *
    * @see Resp3Response#string(byte[], ByteBufPool)
    * @see Resp3Response#string(CharSequence, ByteBufPool)
    */
   BULK_STRING {
      @Override
      public void serialize(Object object, ByteBufPool alloc) {
         Resp3SerializerRegistry.serialize(object, alloc, PrimitiveSerializer.BULK_STRING_SERIALIZERS);
      }
   },

   /**
    * Integer numbers represented by 64-bits.
    *
    * @see Resp3Response#integers(Number, ByteBufPool)
    */
   INTEGER {
      @Override
      public void serialize(Object object, ByteBufPool alloc) {
         Resp3Response.integers((Number) object, alloc);
      }
   },

   /**
    * Rational numbers represented by float or double.
    *
    * @see Resp3Response#doubles(Number, ByteBufPool)
    */
   DOUBLE {
      @Override
      public void serialize(Object object, ByteBufPool alloc) {
         Resp3Response.doubles((Number) object, alloc);
      }
   };

   /**
    * Delegates the serialization to a specific method.
    *
    * <p>
    * This approach avoids introspecting all the registered serializes in the system. In the worst case, it defaults
    * to the unknown approach, and traverse the complete serializer list.
    * </p>
    *
    * @param object The element to serialize.
    * @param alloc The allocator to utilize.
    */
   @Override
   public abstract void serialize(Object object, ByteBufPool alloc);
}
