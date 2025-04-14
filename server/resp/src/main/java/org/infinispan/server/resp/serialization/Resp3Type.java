package org.infinispan.server.resp.serialization;

import java.util.Collection;
import java.util.Map;

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
 * utilize the appropriate method in {@link ResponseWriter} and provide a {@link JavaObjectSerializer} to serialize.
 * </p>
 *
 * @author José Bolina
 * @see <a href="https://github.com/antirez/RESP3/blob/master/spec.md#resp3-types/">RESP3 Types</a>
 */
public enum Resp3Type implements SerializationHint.SimpleHint {
   /**
    * Strings which do not contain any of the escape characters ('\n' or '\r').
    *
    * @see ResponseWriter#simpleString(CharSequence)
    */
   SIMPLE_STRING {
      @Override
      public void serialize(Object object, ResponseWriter writer) {
         writer.simpleString((CharSequence) object);
      }
   },

   /**
    * Any type of string.
    *
    * @see ResponseWriter#string(byte[])
    * @see ResponseWriter#string(CharSequence)
    */
   BULK_STRING {
      @Override
      public void serialize(Object object, ResponseWriter writer) {
         if (object instanceof byte[]) {
            writer.string((byte[]) object);
         } else {
            writer.string((CharSequence) object);
         }
      }
   },

   /**
    * Integer numbers represented by 64-bits.
    *
    * @see ResponseWriter#integers(Number)
    */
   INTEGER {
      @Override
      public void serialize(Object object, ResponseWriter writer) {
         writer.integers((Number) object);
      }
   },

   /**
    * Rational numbers represented by float or double.
    *
    * @see ResponseWriter#doubles(Number)
    */
   DOUBLE {
      @Override
      public void serialize(Object object, ResponseWriter writer) {
         writer.doubles((Number) object);
      }
   },

   AUTO {
      @Override
      public void serialize(Object object, ResponseWriter writer) {
         if (object instanceof CharSequence s) {
            writer.string(s);
         } else if (object instanceof Number n) {
            writer.integers(n);
         } else if (object instanceof Collection<?> c) {
            writer.array(c, AUTO);
         } else if (object instanceof Map<?,?> m) {
            writer.map(m, AUTO);
         } else {
            throw new UnsupportedOperationException();
         }
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
    * @param writer The allocator to utilize.
    */
   @Override
   public abstract void serialize(Object object, ResponseWriter writer);
}
